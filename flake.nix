{
  description = "dev env for sqlite4clj";
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    flakelight.url = "github:nix-community/flakelight";
    flakelight.inputs.nixpkgs.follows = "nixpkgs";
    zig.url = "github:mitchellh/zig-overlay";
  };
  outputs =
    {
      self,
      flakelight,
      zig,
      ...
    }:
    flakelight ./. {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      nixpkgs.config.allowUnsupportedSystem = true;
      devShell =
        pkgs:
        let
          javaVersion = "25";
          jdk = pkgs."jdk${javaVersion}";
          clojure = pkgs.clojure.override { inherit jdk; };
          zigpkgs = zig.packages.${pkgs.system};
        in
        {
          packages = [
            zigpkgs."0.15.2"
            clojure
            jdk
            pkgs.clojure-lsp
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.babashka
            pkgs.git
          ];
        };
      flakelight.builtinFormatters = false;
      formatters = pkgs: {
        "*.nix" = "${pkgs.nixfmt}/bin/nixfmt";
        "*.clj" = "${pkgs.cljfmt}/bin/cljfmt fix";
      };
    };
}
