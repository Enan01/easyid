#!/bin/bash

go build -o easyid

nohup ./easyid -idx=0 -name=node0 -gport=9527 -hport=8000 > /tmp/easyid-node0.log 2>&1 &
nohup ./easyid -idx=1 -name=node1 -gport=9528 -hport=8001 > /tmp/easyid-node1.log 2>&1 &
nohup ./easyid -idx=2 -name=node2 -gport=9529 -hport=8002 > /tmp/easyid-node2.log 2>&1 &
