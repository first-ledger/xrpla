#!/usr/bin/env bash
set -euo pipefail

# Generate reproducible local fixtures; no public ledger is represented here.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${XRPLA_CORE_TEST_BINARY:-$root/build-core/Release/xrpla_core_tests}"
destination="${1:-$root/.cache/fixtures-v2}"
XRPLA_CORE_FIXTURE_DIR="$destination" "$binary" --gtest_filter=CoreFixture.Export
printf 'Canonical synthetic archive written to %s\n' "$destination"
