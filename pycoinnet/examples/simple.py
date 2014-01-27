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

from pycoin.convention import satoshi_to_btc
from pycoin.serialize import b2h_rev
from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


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


def get_msg_version_parameters(transport):
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


def simple_clientbitcoin_peer_protocol(event_loop, address_db, connections, mempool):
    magic_header = binascii.unhexlify('0B110907')  # testnet3
    magic_header = binascii.unhexlify('F9BEB4D9')
    host, port = address_db.next_address()
    logging.info("connecting to %s port %d", host, port)
    try:
        transport, protocol = yield from event_loop.create_connection(
            lambda: BitcoinPeerProtocol(magic_header),
            host=host, port=port)
    except Exception:
        logging.error("failed to connect to %s:%d", host, port)
        address_db.remove_address(host, port)
        address_db.save()
        return

    try:
        address_db.add_address(host, port, int(time.time()))
        connections.add(transport)
        d = get_msg_version_parameters(protocol.transport)
        protocol.send_msg_version(**d)
        message_name, data = yield from protocol.next_message()
        if message_name != 'version':
            raise Exception("missing version")
        protocol.send_msg_verack()
        #protocol.send_msg_getaddr()

        ping_nonces = set()
        def heartbeat():
            while True:
                now = time.time()
                if protocol.last_message_timestamp + 60 < now:
                    # we need to ping!
                    nonce = struct.unpack("!Q", os.urandom(8))[0]
                    protocol.send_msg_ping(nonce)
                    logging.debug("sending ping %d", nonce)
                    ping_nonces.add(nonce)
                    yield from asyncio.sleep(30)
                    if nonce in ping_nonces:
                        # gotta hang up!
                        transport.close()
                        return
                yield from asyncio.sleep(protocol.last_message_timestamp + 60 - now)

        asyncio.Task(heartbeat())

        while True:
            message_name, data = yield from protocol.next_message()
            print("==>", message_name, str(data)[:77])

            if message_name == 'ping':
                print("got ping %s" % data)
                protocol.send_msg_pong(data["nonce"])

            if message_name == 'pong':
                print("got pong %s" % data)
                ping_nonces.discard(data["nonce"])

            if message_name == 'addr':
                address_db.add_addresses(
                    (timestamp, address.ip_address.exploded, address.port)
                    for timestamp, address in data["date_address_tuples"])
                address_db.save()
                #break

            if message_name == 'inv':
                print("inv : %s" % data)
                items = data["items"]
                to_fetch = [item for item in items if item.data not in mempool]
                if to_fetch:
                    protocol.send_msg_getdata(to_fetch)

            if message_name == 'tx':
                tx = data["tx"]
                the_hash = tx.hash()
                if not the_hash in mempool:
                    print("\nTx ID %s" % b2h_rev(the_hash))
                    mempool[the_hash] = tx
                    for idx, tx_out in enumerate(tx.txs_out):
                        ba = tx_out.bitcoin_address()
                        if ba:
                            print("%d: %s %s BTC" % (idx, ba, satoshi_to_btc(tx_out.coin_value)))
                        else:
                            print("can't figure out destination of tx_out id %d" % idx)

            if message_name == 'block':
                block = data["block"]
                the_hash = block.hash()
                mempool[the_hash] = block
                for tx in block.txs:
                    the_hash = tx.hash()
                    print("\nTx ID %s" % b2h_rev(the_hash))
                    for idx, tx_out in enumerate(tx.txs_out):
                        ba = tx_out.bitcoin_address()
                        if ba:
                            print("%d: %s %s BTC" % (idx, ba, satoshi_to_btc(tx_out.coin_value)))
                        else:
                            print("can't figure out destination of tx_out id %d" % idx)

    except Exception:
        logging.exception("problem on %s:%d", host, port)
    transport.close()
    connections.remove(transport)


def keep_minimum_connections(event_loop, min_connection_count=6):
    connections = set()
    address_db = AddressDB("addresses.txt")
    mempool = {}
    while 1:
        logging.debug("checking connection count (currently %d)", len(connections))
        difference = min_connection_count - len(connections)
        for i in range(difference+4):
            asyncio.Task(simple_clientbitcoin_peer_protocol(event_loop, address_db, connections, mempool))
        yield from asyncio.sleep(10)


def main():
    logging.basicConfig(level=logging.DEBUG)
    event_loop = asyncio.get_event_loop()
    asyncio.Task(keep_minimum_connections(event_loop))
    event_loop.run_forever()

main()
