#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <file> <test_number> <myNodeID>" >&2
  echo "Example: $0 tmp.txt 99 48" >&2
  exit 1
}

[[ $# -eq 3 ]] || usage

file="$1"
test_num="$2"
node_id="$3"

[[ -f "$file" ]] || { echo "Error: file not found: $file" >&2; exit 1; }

test_str="TestLongRunningReplication/replication#${test_num}"
node_str="\"myNodeID\": ${node_id}"

tmp_filtered="$(mktemp "${file}.filtered.XXXXXX")"

# Keep only lines that contain BOTH the test string and the node string
grep -F "$test_str" "$file" | grep -F "$node_str" > "$tmp_filtered"

# Replace original file in place (atomic-ish)
mv "$tmp_filtered" "$file"
