#!/usr/bin/env python

from setuptools import setup

version = "0.10"

setup(
    name="pycoinnet",
    version=version,
    packages=[
        "pycoinnet",
        "pycoinnet.helpers",
        "pycoinnet.peer",
        "pycoinnet.peer.tests",
        "pycoinnet.peergroup",
        "pycoinnet.util",
    ],
    entry_points = { 'console_scripts':
        [
            'blockwatcher = pycoinnet/examples/blockwatcher.py:main',
        ]
    },
    author="Richard Kiss",
    author_email="him@richardkiss.com",
    url="https://github.com/richardkiss/pycoinnet",
    license="http://opensource.org/licenses/MIT",
    description="Network utilities for communicating on the bitcoin network."
)
