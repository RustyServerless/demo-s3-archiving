{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
  };
  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem
    (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        dependencies = with pkgs; [
          jdk21 # For local-dynamoddb
          awscli2 # For interacting with local-dynamodb
          jq # For pretty printing X-Ray traces
          nodejs_24 # For frontend dev
          python313Packages.boto3 # Allow Python scripts for interacting with AWS
          cargo-lambda # Run lambdas locally
          socat # Receive X-Ray UDP traces in local testing
        ];
      in
        with pkgs; {
          devShells.default = mkShell {
            buildInputs = [rustToolchain dependencies];
            RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          };
        }
    );
}
