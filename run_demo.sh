#!/usr/bin/env bash
set -e
python server.py --id 1 --client-port 10001 --server-port 11001 &
python server.py --id 2 --client-port 10002 --server-port 11002 &
python server.py --id 3 --client-port 10003 --server-port 11003 &
wait
