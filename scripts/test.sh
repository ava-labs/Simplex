#!/usr/bin/env bash
set -euo pipefail

# Run tests with JSON output and extract only the failed test logs
go test -race -json ./... | tee test_output.json | jq -r '
  select(.Action=="fail") | 
  "FAIL: \(.Test // .Package)\n\(.Output // "")"
'

# Fail the script if any test failed
if grep -q '"Action":"fail"' test_output.json; then
  exit 1
fi
