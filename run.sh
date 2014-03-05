#!/bin/sh

PYTHONPATH=`pwd` pycoinnet/examples/blockwatcher.py -c config -f 288900 -l blockwatcher.log blockstore
