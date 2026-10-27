#!/usr/bin/env bash
# Usage: scripts/sync-contracts.sh <data-contracts commit sha>
set -euo pipefail
sha="${1:?usage: $0 <data-contracts commit sha>}"
repo="${DATA_CONTRACTS_REPO:-https://github.com/reillym11/data-contracts.git}"
root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

git clone -q "$repo" "$tmp/src"
git -C "$tmp/src" checkout -q "$sha"
full="$(git -C "$tmp/src" rev-parse HEAD)"

rm -rf "$root/contracts"
mkdir -p "$root/contracts/fixtures"
cp -R "$tmp/src/contracts/." "$root/contracts/"
cp -R "$tmp/src/fixtures/sample" "$root/contracts/fixtures/sample"
echo "$full" > "$root/contracts/SOURCE"
echo "vendored data-contracts@$full"
