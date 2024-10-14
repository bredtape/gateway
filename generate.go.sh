#!/bin/sh

dir=$(dirname $1)
file=$(basename $1)
echo "Generating code for $1"

protoc \
    --proto_path=${dir} \
    --go_out=${dir} \
    --go_opt=paths=source_relative \
    --go-grpc_out=${dir} \
    --go-grpc_opt=paths=source_relative \
    ${file}
