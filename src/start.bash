#!/usr/bin/env bash

pub=twitter

##
## make sure your packages actually live in these locations
##
export PYTHONPATH=${PYTHONPATH}:~/Gnip-Stream-Collector-Metrics/src:~/Gnacs/acscsv

nohup ./start.py --name=$pub &

tail -f ./nohup.out