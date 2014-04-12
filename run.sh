#!/bin/sh

PYTHONPATH=`pwd` pycoinnet/examples/blockwatcher.py -s blockwatcher_state -f 288900 -l blockwatcher.log blockstore
