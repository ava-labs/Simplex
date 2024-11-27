#!/usr/bin/env -euxo pipefail bash

grep -oe "Fuzz.*(f \*" * | cut -d\( -f1 | awk -F: '{print $2}' | while read testName; do
  echo ${testName}
  go test -fuzz="${testName}" -fuzztime 10s
done