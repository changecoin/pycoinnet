#!/usr/bin/env python

"""
This bitcoin client does little more than try to keep an up-to-date
list of available clients in a text file "addresses".
"""

import asyncio
import logging
import random
import time

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol


class AddressKeeperPeer(BitcoinPeerProtocol):
    def __init__(self, client):
        self.client = client

    def connection_made(self, transport):
        super(AddressKeeperPeer, self).connection_made(transport)
        self.client.handle_peer_connected(self)

    def connection_lost(self, exc_or_none):
        self.client.handle_peer_lost(self, exc_or_none)

    def handle_msg_verack(self):
        # TODO: kick of getaddr timer
        print("got verack")
        self.write_message(b"getaddr")

    def handle_msg_addr(self, date_address_tuples):
        self.client.add_addresses(date_address_tuples)

    def handle_msg_inv(self, items):
        print("got %d items" % len(items))
        print(items)

    def handle_msg_tx(self, tx):
        print("got tx %s" % repr(tx))

    def handle_msg_alert(self, payload, signature, alert_msg):
        print("alert statusBar: %s" % alert_msg["statusBar"].decode("utf8"))


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


class AddressKeeperClient(object):
    def __init__(self, event_loop, min_connection_count=2):
        self.connections = set()
        self.min_connection_count = min_connection_count
        self.address_db = AddressDB("addresses.txt")
        self.event_loop = event_loop
        asyncio.Task(self.check_connection_count())

    @asyncio.coroutine
    def attempt_connection(self):
        host, port = self.address_db.next_address()
        logging.info("connecting to %s port %d", host, port)
        try:
            peer = AddressKeeperPeer(self)
            yield from self.event_loop.create_connection(
                lambda: peer, host=host, port=port)
            self.address_db.add_address(host, port, int(time.time()))
        except Exception:
            logging.error("failed to connect to %s:%d", host, port)
            self.address_db.remove_address(host, port)
            self.address_db.save()

    @asyncio.coroutine
    def check_connection_count(self):
        while 1:
            logging.debug("checking connection count (currently %d)", len(self.connections))
            difference = self.min_connection_count - len(self.connections)
            for i in range(difference*3):
                asyncio.Task(self.attempt_connection())
            yield from asyncio.sleep(10)

    def handle_peer_connected(self, peer):
        self.connections.add(peer)

    def handle_peer_lost(self, peer, exc_or_none):
        self.connections.remove(peer)

    def add_addresses(self, date_address_tuples):
        self.address_db.add_addresses(
            (timestamp, address.ip_address.exploded, address.port)
            for timestamp, address in date_address_tuples)
        self.address_db.save()


def main():
    logging.basicConfig(level=logging.DEBUG)
    event_loop = asyncio.get_event_loop()
    AddressKeeperClient(event_loop)
    event_loop.run_forever()

main()
