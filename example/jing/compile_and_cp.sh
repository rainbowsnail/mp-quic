#!/bin/bash

go build main.go
cp main /home/mininet/git/mininet-multipath/network/server

cd client
go build main.go
cp main /home/mininet/git/mininet-multipath/network/client

cd ..
