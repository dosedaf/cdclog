# Process definitions for the CDC stack, wired into the flake via
# process-compose-flake (see flake.nix). Rendered as a plain module attrset.
{
  pkgs,
  lib,
  shadowtrafficImage,
}: let
  # Debezium is not packaged in nixpkgs; it is just a bundle of Kafka Connect
  # plugin jars, so we fetch the official Maven Central distribution.
  debeziumVersion = "2.7.3.Final";

  pgPort = 5433; # non-default to avoid clashing with a system postgres
  kafkaPort = 9092;
  kafkaControllerPort = 9093;
  connectPort = 8083;
  pgDb = "cdcdemo";
  pgUser = "cdc";
  pgPassword = "cdc";

  # ShadowTraffic is proprietary and ships as a container; the flake pins the
  # image by digest (dockerTools.pullImage) and we run it with the local
  # container runtime. Swap to podman here if you prefer rootless.
  containerRuntime = "${pkgs.docker}/bin/docker";
  # Tag restored by `docker load` from the pinned image; keep in sync with
  # finalImageTag in flake.nix.
  stImageRef = "shadowtraffic/shadowtraffic:2.3.2";
  stWorkload = ./shadowtraffic-workload.json;

  # attr name changed between nixpkgs versions; support both spellings
  kafka = pkgs.apacheKafka or pkgs.apache-kafka;
  postgresql = pkgs.postgresql;
  # version attr includes the Scala compiler prefix, e.g. "2.13-4.3.1";
  # strip it and compare the Kafka major version.
  kafkaMajor = lib.toInt (lib.head (lib.splitVersion (lib.last (lib.splitString "-" kafka.version))));
  kafka4 = kafkaMajor >= 4;

  debeziumTarball = pkgs.fetchurl {
    url = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${debeziumVersion}/debezium-connector-postgres-${debeziumVersion}-plugin.tar.gz";
    sha256 = "9bf3f06419d30c57eb9d0d2e717f8148bcf35eb174e1dc7f1acc182da803d9f1";
  };

  # Kafka Connect expects each connector in its own subdirectory of a
  # plugin.path entry. The Debezium tarball already extracts to
  # debezium-connector-postgres/, so a plain unpack gives us exactly that.
  debeziumPlugins = pkgs.runCommand "debezium-connector-postgres-${debeziumVersion}" {
    nativeBuildInputs = [ pkgs.gnutar ];
    meta.description = "Debezium PostgreSQL connector plugin for Kafka Connect";
  } ''
    mkdir -p $out
    tar xzf ${debeziumTarball} -C $out
  '';

  # Kafka Connect worker config (standalone mode). plugin.path points at the
  # read-only store path with the Debezium jars; offsets live in .data/.
  workerProps = pkgs.writeText "connect-standalone.properties" ''
    bootstrap.servers=127.0.0.1:${toString kafkaPort}
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false
    offset.storage.file.filename=.data/connect/offsets.dat
    offset.flush.interval.ms=1000
    # `hybrid` was the transitional value in Kafka 3.6-3.x; Kafka 4.x only
    # accepts [only_scan, hybrid_warn, hybrid_fail, service_load].
    plugin.discovery=${if kafka4 then "hybrid_warn" else "hybrid"}
    plugin.path=${debeziumPlugins}
    listeners=http://127.0.0.1:${toString connectPort}
  '';

  # The Debezium PostgreSQL connector, loaded by connect-standalone at boot.
  connectorProps = pkgs.writeText "cdc-postgres-connector.properties" ''
    name=cdc-postgres
    connector.class=io.debezium.connector.postgresql.PostgresConnector
    database.hostname=127.0.0.1
    database.port=${toString pgPort}
    database.user=${pgUser}
    database.password=${pgPassword}
    database.dbname=${pgDb}
    topic.prefix=cdcdemo
    plugin.name=pgoutput
    slot.name=cdc_slot
    publication.autocreate.mode=all_tables
    table.include.list=public.customers,public.products,public.orders,public.order_items
    decimal.handling.mode=string
    snapshot.mode=initial
    tombstones.on.delete=false
    key.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false
  '';

  # One-shot: initialize the PostgreSQL cluster (data dir, logical WAL).
  pgInit = pkgs.writeShellScript "pg-init" ''
    set -euo pipefail
    data="$PWD/.data"
    mkdir -p "$data/pg-sock"
    if [ ! -f "$data/pg/PG_VERSION" ]; then
      ${postgresql}/bin/initdb -D "$data/pg" -U postgres -A trust --encoding=UTF8
      cat >> "$data/pg/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = ${toString pgPort}
unix_socket_directories = '$data/pg-sock'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = '64MB'
EOF
    fi
  '';

  # One-shot: replication role, demo database, and the CDC source schema:
  # a 4-table e-commerce model (dimensions seeded by ShadowTraffic, orders
  # mutated through a lifecycle). Relationships are LOGICAL only — no DB-level
  # FK constraints. ShadowTraffic resolves FKs via random lookups over its
  # event history, which can briefly reference rows that were already
  # deleted; DB enforcement would turn that into a fatal error for the
  # generator. The CDC pipeline cares about event shapes, not enforced
  # integrity. ids are client-generated by the workload (UPDATE/DELETE need
  # known ids), hence plain bigint and no sequences.
  pgSchema = pkgs.writeShellScript "pg-schema" ''
    set -euo pipefail
    psql="${postgresql}/bin/psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -p ${toString pgPort} -U postgres"
    # SUPERUSER is a dev shortcut: it lets the cdc role create the all-tables
    # publication that Debezium's pgoutput mode expects. REPLICATION is set
    # explicitly because Debezium's connection check inspects the role
    # attribute itself, even though superusers bypass it server-side.
    if ! $psql -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '${pgUser}'" | grep -q 1; then
      $psql -d postgres -c "CREATE ROLE ${pgUser} LOGIN SUPERUSER REPLICATION PASSWORD '${pgPassword}'"
    fi
    $psql -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${pgDb}'" | grep -q 1 \
      || $psql -d postgres -c "CREATE DATABASE ${pgDb} OWNER ${pgUser}"
    $psql -d ${pgDb} <<'SQL'
      CREATE TABLE IF NOT EXISTS customers (
        id          bigint PRIMARY KEY,
        name        text NOT NULL,
        email       text NOT NULL,
        country     text NOT NULL,
        segment     text NOT NULL,
        created_at  timestamptz NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS products (
        id      bigint PRIMARY KEY,
        sku     text NOT NULL,
        name    text NOT NULL,
        price   numeric(10,2) NOT NULL,
        stock   integer NOT NULL DEFAULT 0,
        attrs   jsonb NOT NULL DEFAULT '{}'::jsonb
      );
      CREATE TABLE IF NOT EXISTS orders (
        id          bigint PRIMARY KEY,
        customer_id bigint NOT NULL, -- logically references customers
        status      text NOT NULL,
        total       numeric(12,2) NOT NULL DEFAULT 0,
        placed_at   timestamptz NOT NULL DEFAULT now(),
        updated_at  timestamptz NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS order_items (
        id         bigint PRIMARY KEY,
        order_id   bigint NOT NULL,   -- logically references orders
        product_id bigint NOT NULL,   -- logically references products
        quantity   integer NOT NULL,
        unit_price numeric(10,2) NOT NULL
      );
      -- Dev-stack semantics: every boot starts from empty tables so
      -- ShadowTraffic's deterministic stage-1 seeding (ids restart at their
      -- startingFrom each run) can never collide with rows left over from a
      -- previous boot or a generator restart. CDC history lives in Kafka,
      -- not here.
      TRUNCATE customers, products, orders, order_items;
SQL
  '';

  # One-shot: generate KRaft server.properties and format the storage dir once.
  kafkaInit = pkgs.writeShellScript "kafka-init" ''
    set -euo pipefail
    data="$PWD/.data"
    mkdir -p "$data/kafka" "$data/kafka-conf" "$data/logs/kafka" "$data/connect"
    conf="$data/kafka-conf/server.properties"
    if [ ! -f "$data/kafka/meta.properties" ]; then
      cluster_id=$(${kafka}/bin/kafka-storage.sh random-uuid)
      cat > "$conf" <<EOF
process.roles=broker,controller
node.id=1
${lib.optionalString kafka4 ''
controller.quorum.bootstrap.servers=127.0.0.1:${toString kafkaControllerPort}
share.coordinator.state.topic.replication.factor=1
''}
${lib.optionalString (!kafka4) ''
controller.quorum.voters=1@127.0.0.1:${toString kafkaControllerPort}
''}
listeners=PLAINTEXT://127.0.0.1:${toString kafkaPort},CONTROLLER://127.0.0.1:${toString kafkaControllerPort}
advertised.listeners=PLAINTEXT://127.0.0.1:${toString kafkaPort}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$data/kafka
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
EOF
      ${kafka}/bin/kafka-storage.sh format -t "$cluster_id" -c "$conf" ${lib.optionalString kafka4 "--standalone"}
    fi
  '';

  # One-shot: wait until Debezium reports the connector RUNNING.
  connectorReady = pkgs.writeShellScript "connector-ready" ''
    set -euo pipefail
    status=""
    for i in $(seq 1 60); do
      status=$(curl -sf http://127.0.0.1:${toString connectPort}/connectors/cdc-postgres/status || true)
      if echo "$status" | grep -q '"RUNNING"'; then
        echo "cdc-postgres connector is RUNNING"
        exit 0
      fi
      sleep 2
    done
    echo "cdc-postgres did not reach RUNNING state; last status: $status"
    exit 1
  '';

  # One-shot: load the pinned ShadowTraffic image into the local daemon.
  # After this, the daemon never contacts a registry — everything comes from
  # the Nix store.
  stInit = pkgs.writeShellScript "st-init" ''
    set -euo pipefail
    echo "loading pinned ShadowTraffic image into ${containerRuntime}..."
    ${containerRuntime} load -i ${shadowtrafficImage}
  '';

  # Long-running: ShadowTraffic generating the e-commerce workload.
  # Networking note: Docker Desktop's `--network host` does NOT share the host
  # network namespace (containers run in a VM), so 127.0.0.1 would be wrong.
  # We stay on the default bridge and reach the host's Postgres via the
  # `host.docker.internal` alias (--add-host makes this portable to plain
  # Docker on Linux too). The config is passed base64-encoded on the CLI
  # instead of a bind mount: Docker Desktop only mounts whitelisted paths,
  # and /nix/store isn't one. license.env is sourced by docker at runtime
  # from $PWD; it must never be committed or copied into the Nix store.
  stRun = pkgs.writeShellScript "st-run" ''
    set -euo pipefail
    if [ ! -f "$PWD/license.env" ]; then
      echo "error: license.env not found in $PWD" >&2
      echo "       get a free trial at https://shadowtraffic.io/pricing.html" >&2
      echo "       and save it as license.env (LICENSE_ID=... LICENSE_SIGNATURE=...)" >&2
      exit 1
    fi
    exec ${containerRuntime} run --rm --name cdc-shadowtraffic \
      --add-host=host.docker.internal:host-gateway \
      --env-file "$PWD/license.env" \
      ${stImageRef} \
      --config-base64 "$(${pkgs.coreutils}/bin/base64 -w0 ${stWorkload})"
  '';
in {
  cli = {
    preHook = ''
      mkdir -p .data
    '';
    environment.PC_ORDERED_SHUTDOWN = true;
  };

  settings.processes = {
    pg-init = {
      command = "${pgInit}";
    };

    postgres = {
      command = "${postgresql}/bin/postgres -D $PWD/.data/pg";
      depends_on.pg-init.condition = "process_completed_successfully";
      availability = {
        restart = "on_failure";
        max_restarts = 3;
      };
      readiness_probe = {
        exec.command = "${postgresql}/bin/pg_isready -h 127.0.0.1 -p ${toString pgPort} -U postgres";
        initial_delay_seconds = 2;
        period_seconds = 2;
        timeout_seconds = 2;
        failure_threshold = 30;
      };
    };

    pg-schema = {
      command = "${pgSchema}";
      depends_on.postgres.condition = "process_healthy";
    };

    kafka-init = {
      command = "${kafkaInit}";
    };

    kafka = {
      command = "${kafka}/bin/kafka-server-start.sh $PWD/.data/kafka-conf/server.properties";
      environment = {
        LOG_DIR = ".data/logs/kafka";
        KAFKA_HEAP_OPTS = "-Xms256m -Xmx768m";
      };
      depends_on.kafka-init.condition = "process_completed_successfully";
      availability = {
        restart = "on_failure";
        max_restarts = 3;
      };
      readiness_probe = {
        exec.command = "${kafka}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:${toString kafkaPort} --list";
        initial_delay_seconds = 3;
        period_seconds = 3;
        timeout_seconds = 10;
        failure_threshold = 40;
      };
    };

    connect = {
      command = "mkdir -p .data/connect && exec ${kafka}/bin/connect-standalone.sh ${workerProps} ${connectorProps}";
      environment = {
        LOG_DIR = ".data/logs/connect";
        KAFKA_HEAP_OPTS = "-Xms128m -Xmx512m";
      };
      depends_on.kafka.condition = "process_healthy";
      availability = {
        restart = "on_failure";
        max_restarts = 3;
      };
      readiness_probe = {
        exec.command = "curl -sf http://127.0.0.1:${toString connectPort}/connectors";
        initial_delay_seconds = 5;
        period_seconds = 3;
        timeout_seconds = 5;
        failure_threshold = 60;
      };
    };

    connector-ready = {
      command = "${connectorReady}";
      depends_on.connect.condition = "process_healthy";
    };

    st-init = {
      command = "${stInit}";
    };

    shadowtraffic = {
      command = "${stRun}";
      depends_on = {
        st-init.condition = "process_completed_successfully";
        connector-ready.condition = "process_completed_successfully";
      };
      availability = {
        restart = "on_failure";
        max_restarts = 2; # license/config errors shouldn't loop forever
      };
    };
  };
}
