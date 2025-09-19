#!/usr/bin/env -euxo pipefail bash

go test -v -race ./... | tee out.log 

grep -Eq "ERR|WARN" out.log 
if [[ $? -eq 0 ]];then
	echo "Found warnings or errors in the test run"
	exit 1
fi

