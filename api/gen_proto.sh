#!/usr/bin/env bash

protoc --go.official_out=plugins=grpc:. ./api/id.proto