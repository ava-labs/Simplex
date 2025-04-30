#!/usr/bin/env -euxo pipefail bash


function dots() {
    while :; do
      echo "."
      sleep 1
    done
}

dots &

go test -race ./...