#!/usr/bin/env -euxo pipefail bash

grep -ore "Fuzz.*(f \*" * | cut -d\( -f1 | while read line; do
  file=$( echo $line | cut -d: -f1 )
  testName=$( echo $line | cut -d: -f2  )
  dir=$( dirname $file )
  cd $dir
  go test -fuzz="${testName}" -fuzztime 10s
  cd -
done
