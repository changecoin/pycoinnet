#!/usr/bin/env python

"""
Custom bitcoin client
"""

import asyncio
import logging

from asyncio.queues import Queue

from pycoinnet.util import ConnectionManager


ADDRESS_QUEUE = Queue()

@asyncio.coroutine
def fetch_addresses(event_loop):
    for h in [
        "bitseed.xf2.org", "dnsseed.bluematt.me",
        "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org"
    ]:
        r = yield from event_loop.getaddrinfo(h, 8333)
        results = set(t[-1][:2] for t in r)
        for t in results:
            yield from ADDRESS_QUEUE.put(t)
            logging.debug("got address %s", t)

def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    event_loop = asyncio.get_event_loop()
    cm = ConnectionManager.ConnectionManager(ADDRESS_QUEUE, ConnectionManager.MAINNET_MAGIC_HEADER)
    cm.run()
    asyncio.Task(fetch_addresses(event_loop))
    event_loop.run_forever()

main()
