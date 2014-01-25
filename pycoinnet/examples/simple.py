#!/usr/bin/env python

"""
This bitcoin client does little more than try to keep an up-to-date
list of available clients in a text file "addresses".
"""

import asyncio
import binascii
import logging
import os
import random
import struct
import time

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol


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


def get_msg_version_parameters_default(transport):
    # this must return a dictionary with:
    #  version (integer)
    #  subversion (bytes, like b"/Satoshi:0.7.2/")
    #  node_type (must be 1)
    #  current time (seconds since epoch)
    #  remote_address
    #  remote_listen_port
    #  local_address
    #  local_listen_port
    #  nonce (32 bit)
    #  last_block_index
    remote_address, remote_port = transport.get_extra_info("socket").getpeername()
    return dict(
        version=70001,
        subversion=b"/Notoshi/",
        node_type=1,
        current_time=int(time.time()),
        remote_address=remote_address,
        remote_listen_port=remote_port,
        local_address="127.0.0.1",
        local_listen_port=6111,
        nonce=struct.unpack("!Q", os.urandom(8))[0],
        last_block_index=0,
    )


def simple_clientbitcoin_peer_protocol(event_loop, address_db, connections):
    magic_header = binascii.unhexlify('0B110907')
    host, port = address_db.next_address()
    logging.info("connecting to %s port %d", host, port)
    try:
        transport, protocol = yield from event_loop.create_connection(
            lambda: BitcoinPeerProtocol(magic_header),
            host=host, port=port)
        address_db.add_address(host, port, int(time.time()))
        connections.add(transport)
    except Exception:
        logging.error("failed to connect to %s:%d", host, port)
        address_db.remove_address(host, port)
        address_db.save()
        return

    try:
        d = get_msg_version_parameters_default(protocol.transport)
        protocol.send_msg_version(**d)
        message_name, data = yield from protocol.next_message()
        print(message_name, data)
        if message_name != 'version':
            raise Exception("missing version")
        protocol.send_msg_verack()
        protocol.send_msg_getaddr()
        while True:
            message_name, data = yield from protocol.next_message()
            if message_name == 'addr':
                address_db.add_addresses(
                    (timestamp, address.ip_address.exploded, address.port)
                    for timestamp, address in data["date_address_tuples"])
                address_db.save()
    except Exception:
        logging.exception("problem on %s:%d", host, port)
    connections.remove(transport)


def keep_minimum_connections(event_loop, min_connection_count=3):
    connections = set()
    address_db = AddressDB("addresses.txt")
    while 1:
        logging.debug("checking connection count (currently %d)", len(connections))
        difference = min_connection_count - len(connections)
        for i in range(difference*3):
            asyncio.Task(simple_clientbitcoin_peer_protocol(event_loop, address_db, connections))
        yield from asyncio.sleep(10)


def main():
    logging.basicConfig(level=logging.DEBUG)
    event_loop = asyncio.get_event_loop()
    asyncio.Task(keep_minimum_connections(event_loop))
    event_loop.run_forever()

main()
