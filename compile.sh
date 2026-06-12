#!/usr/bin/env bash
GOPATH="${1:-"${HOME}/go"}"
export GOPATH

go build -o udp