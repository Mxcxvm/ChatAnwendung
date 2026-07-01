#!/usr/bin/env bash


python client.py --username manu
python server.py --client-port 60975 --server-port 60974
python server.py --client-port 60973 --server-port 61000