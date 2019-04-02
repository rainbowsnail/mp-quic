#!/bin/bash

GOOS=linux GOARCH=amd64 go build -o $1/client ./example/tiny/client
GOOS=linux GOARCH=amd64 go build -o $1/server ./example/tiny
