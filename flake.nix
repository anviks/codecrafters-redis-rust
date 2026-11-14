{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
    in
    {
      devShells.x86_64-linux.default = pkgs.mkShell {
        packages = with pkgs; [
          cargo
          rustc
          rust-analyzer
          clippy
          rustfmt
          codecrafters-cli
          redis
        ];
        RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
      };
    };
}
