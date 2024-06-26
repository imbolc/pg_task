#!/usr/bin/env bash
set -eux

# Install tools
cargo clippy --version &>/dev/null || rustup component add clippy
cargo machete --version &>/dev/null || cargo install --locked cargo-machete
cargo sort --version &>/dev/null || cargo install --locked cargo-sort
cargo sqlx --version &>/dev/null || cargo install --locked sqlx-cli
rusty-hook --version &>/dev/null || cargo install --locked rusty-hook
typos --version &>/dev/null || cargo install --locked typos-cli

rustup toolchain list | grep -q 'nightly' || rustup toolchain install nightly
cargo +nightly fmt --version &>/dev/null || rustup component add rustfmt --toolchain nightly

# Checks
typos .
cargo machete
cargo +nightly fmt -- --check
cargo sort -c
cargo test --examples --tests
cargo sqlx prepare && git add .sqlx
cargo sync-readme && git add README.md
cargo clippy --examples --tests -- -D warnings
