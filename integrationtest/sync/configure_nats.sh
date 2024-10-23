#!/bin/bash

set -e

CLI="go run ../../sync/cli/main.go"

# sync A
SERVER="localhost:4222"
$CLI --server="${SERVER}" bootstrap create
$CLI --server="${SERVER}" bootstrap sync a b
nats stream add --server=nats://${SERVER} --subjects="hello.*" --description="dummy hi" --defaults hello

# sync B
SERVER="localhost:4322"
$CLI --server="${SERVER}" bootstrap create
$CLI --server="${SERVER}" bootstrap sync a b
nats stream add --server=nats://${SERVER} --subjects="hello.*" --description="dummy hi" --defaults hello
