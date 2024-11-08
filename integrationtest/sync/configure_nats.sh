#!/bin/bash

set -e
set -x

CLI="go run ../../sync/cli/main.go"

# sync A
SERVER="localhost:4222"
CMD="$CLI --server=${SERVER} --sync_stream=sync1"
$CMD bootstrap create
$CMD bootstrap sync a b
$CMD bootstrap sync a c
nats stream add --server=nats://${SERVER} --subjects="hello.*" --description="dummy hi" --defaults hello
$CMD sync start a b hello
$CMD sync start a c hello

# sync B
SERVER="localhost:4322"
CMD="$CLI --server=${SERVER} --sync_stream=sync2"
$CMD bootstrap create
nats stream add --server=nats://${SERVER} --subjects="hello.*" --description="dummy hi" --defaults hello

# sync C
SERVER="localhost:4422"
CMD="$CLI --server=${SERVER} --sync_stream=sync1"
$CMD bootstrap create
nats stream add --server=nats://${SERVER} --subjects="hello.*" --description="dummy hi" --defaults hello
