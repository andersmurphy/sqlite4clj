{
  description = "dev env for sqlite4clj";
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    flakelight.url = "github:nix-community/flakelight";
    flakelight.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs =
    {
      self,
      flakelight,
      ...
    }:
    flakelight ./. {
      devShell =
        pkgs:
        let
          javaVersion = "24";
          jdk = pkgs."jdk${javaVersion}";
          clojure = pkgs.clojure.override { inherit jdk; };
          sqlite = (
            pkgs.sqlite.overrideAttrs (oldAttrs: {
              env.NIX_CFLAGS_COMPILE = (
                toString [
                  "-DSQLITE_THREADSAFE=2"
                  "-DSQLITE_DEFAULT_MEMSTATUS=0"
                  "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1"
                  "-DSQLITE_LIKE_DOESNT_MATCH_BLOBS"
                  "-DSQLITE_MAX_EXPR_DEPTH=0"
                  "-DSQLITE_OMIT_DECLTYPE"
                  "-DSQLITE_OMIT_DEPRECATED"
                  "-DSQLITE_OMIT_PROGRESS_CALLBACK"
                  "-DSQLITE_OMIT_SHARED_CACHE"
                  "-DSQLITE_USE_ALLOCA"
                  "-DSQLITE_STRICT_SUBTYPE=1"
                  "-DSQLITE_OMIT_AUTOINIT"
                  "-DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS"
                  "-DSQLITE_ENABLE_STAT4"
                ]
              );
            })
          );
          libraries = [ sqlite ];
        in
        {
          packages = [
            sqlite
            clojure
            jdk
            pkgs.clojure-lsp
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.babashka
            pkgs.git
          ];
          env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath libraries;
        };

      flakelight.builtinFormatters = false;
      formatters = pkgs: {
        "*.nix" = "${pkgs.nixfmt}/bin/nixfmt";
        "*.clj" = "${pkgs.cljfmt}/bin/cljfmt fix";
      };
    };
}
