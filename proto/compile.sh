#!/bin/bash
go get google.golang.org/protobuf/cmd/protoc-gen-go \
         google.golang.org/grpc/cmd/protoc-gen-go-grpc

rm -rf ../pb
mkdir ../pb

protoc --go_out=../pb --go-grpc_out=../pb api.proto
