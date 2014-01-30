#!/usr/bin/env python

"""
Custom bitcoin client
"""

import asyncio
import logging

from asyncio.queues import Queue

from pycoinnet.peergroup import ConnectionManager
from pycoinnet.peergroup import InvCollector

ADDRESS_QUEUE = Queue()

import random

class AddressDB(object):
    def __init__(self, path):
        self.path = path
        self.addresses = self.load_addresses()
        self.shuffled = []

    def load_addresses(self):
        """
        Return an array of (host, port, timestamp) triples.
        """
        addresses = {}
        try:
            with open(self.path) as f:
                for l in f:
                    timestamp, host, port = l[:-1].split("/")
                    timestamp = int(timestamp)
                    port = int(port)
                    addresses[(host, port)] = timestamp
        except Exception:
            pass
        return addresses

    def next_address(self):
        if len(self.shuffled) == 0:
            self.shuffled = list(self.addresses.keys())
            random.shuffle(self.shuffled)
        return self.shuffled.pop()

    def remove_address(self, host, port):
        key = (host, port)
        del self.addresses[key]

    def add_address(self, host, port, timestamp):
        key = (host, port)
        old_timestamp = self.addresses.get(key) or timestamp
        self.addresses[key] = max(timestamp, old_timestamp)

    def add_addresses(self, addresses):
        for timestamp, host, port in addresses:
            self.add_address(host, port, timestamp)

    def save(self):
        if len(self.addresses) < 2:
            logging.error("too few addresses: not overwriting")
            return
        with open(self.path, "w") as f:
            for host, port in self.addresses:
                f.write("%d/%s/%d\n" % (self.addresses[(host, port)], host, port))


@asyncio.coroutine
def fetch_addresses(event_loop):
    address_db = AddressDB("addresses.txt")
    for i in range(1000):
        t = address_db.next_address()
        yield from ADDRESS_QUEUE.put(t)
    return
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
    inv_collector = InvCollector.InvCollector()
    cm = ConnectionManager.ConnectionManager(ADDRESS_QUEUE, ConnectionManager.MAINNET_MAGIC_HEADER)
    cm.run()
    asyncio.Task(fetch_addresses(event_loop))
    event_loop.run_forever()

main()
