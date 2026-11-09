#!/usr/bin/env bash
# Re-sync the WAL spec and golden fixtures from the CLIProxyAPI fork, or check
# that they match. The proxy is the source of truth; this repo vendors copies.
#
#   scripts/sync-fixtures.sh check   # exit 1 if the vendored copies differ
#   scripts/sync-fixtures.sh sync    # copy from the proxy checkout and record its commit
#
# PROXY_DIR points at the proxy checkout (default: the sibling router-for-me/CLIProxyAPI).
set -euo pipefail

here="$(cd "$(dirname "$0")/.." && pwd)"
proxy="${PROXY_DIR:-$here/../../router-for-me/CLIProxyAPI}"
mode="${1:-check}"

spec_src="$proxy/docs/archive-wal-format.md"
fixtures_src="$proxy/internal/archive/testdata/wal"
spec_dst="$here/docs/archive-wal-format.md"
fixtures_dst="$here/fixtures/wal"

[ -f "$spec_src" ] || { echo "proxy spec not found at $spec_src (set PROXY_DIR)"; exit 2; }
[ -d "$fixtures_src" ] || { echo "proxy fixtures not found at $fixtures_src (set PROXY_DIR)"; exit 2; }

case "$mode" in
  check)
    status=0
    cmp -s "$spec_src" "$spec_dst" || { echo "spec differs: $spec_dst"; status=1; }
    diff -rq "$fixtures_src" "$fixtures_dst" || status=1
    [ $status -eq 0 ] && echo "spec and fixtures match the proxy checkout"
    exit $status
    ;;
  sync)
    cp "$spec_src" "$spec_dst"
    rm -rf "$fixtures_dst"
    mkdir -p "$fixtures_dst"
    cp "$fixtures_src"/* "$fixtures_dst"/
    commit="$(git -C "$proxy" rev-parse HEAD)"
    branch="$(git -C "$proxy" rev-parse --abbrev-ref HEAD)"
    printf 'repo: https://github.com/tomsanbear/CLIProxyAPI\nbranch: %s\ncommit: %s\nspec: docs/archive-wal-format.md\nfixtures: internal/archive/testdata/wal/\n' "$branch" "$commit" > "$here/docs/WAL-SPEC-SOURCE"
    echo "synced from $branch@$commit"
    ;;
  *)
    echo "usage: $0 [check|sync]"; exit 2
    ;;
esac
