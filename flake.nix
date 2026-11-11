{
  description = "CDC dev stack: ShadowTraffic -> PostgreSQL -> Debezium -> Kafka, orchestrated by Nix";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      imports = [ inputs.process-compose-flake.flakeModule ];

      perSystem = { pkgs, self', ... }: let
        # ShadowTraffic is proprietary and ships only as a Docker image, so Nix
        # pins the image itself: fixed-output download, verified by sha256, and
        # loaded into the local daemon at runtime — nothing is pulled from a
        # registry outside of Nix.
        #
        # Bump procedure: `docker pull shadowtraffic/shadowtraffic:<tag>` (or
        # check hub.docker.com tags), replace imageDigest + finalImageTag, then
        # copy the sha256 the failing build suggests.
        # `latest` == 2.3.2 as of 2026-11-11; prefer the version tag.
        shadowtrafficImage = pkgs.dockerTools.pullImage {
          imageName = "shadowtraffic/shadowtraffic";
          imageDigest = "sha256:840c6b7c4389e9a8c726a4d3215360fcd3be859d168eb9560470073fc8e1ea94";
          finalImageTag = "2.3.2";
          sha256 = "sha256-skCMVxx2sQ8AH0VS+446i0O5c97mKRbieSYJ0OAaU84=";
        };
      in {
        process-compose.cdc = import ./process-compose.nix {
          inherit (pkgs) lib;
          inherit pkgs shadowtrafficImage;
        };

        # `nix run .#` starts the whole stack.
        packages.default = self'.packages.cdc;

        devShells.default = pkgs.mkShell {
          name = "cdc-dev";
          packages = [
            self'.packages.cdc # provides the `cdc` command
            pkgs.apacheKafka # kafka console tools
            pkgs.postgresql # psql, pg_isready
            pkgs.jq
            pkgs.curl
          ];
          shellHook = ''
            echo "cdc dev shell — the whole stack with: cdc"
            echo ""
            echo "  kafka-console-consumer --bootstrap-server localhost:9092 \\"
            echo "      --topic cdcdemo.public.orders --from-beginning"
            echo "  psql -h 127.0.0.1 -p 5433 -U cdc -d cdcdemo"
            echo "  curl -s localhost:8083/connectors/cdc-postgres/status | jq ."
          '';
        };
      };
    };
}
