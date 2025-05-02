#!/usr/bin/env -euxo pipefail bash

function killInBackground() {
	sleep 240
	pkill -SIGABRT simplex
}

function dots() {
    while :; do
      echo "."
      sleep 1
    done
}

killInBackground &

go test ./... -race
