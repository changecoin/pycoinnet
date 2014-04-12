#!/bin/sh

PYTHONPATH=`pwd` pycoinnet/examples/blockwatcher.py -s state_blockwatcher -f 288900 -l blockwatcher.log blockstore
