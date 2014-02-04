#!/usr/bin/env python

"""
Bitcoin client that connects to DNS bootstrap connections, grabs addr records,
disconnects after enough records are obtained, and that's that.
"""

import asyncio
import binascii
import logging

from asyncio.queues import PriorityQueue

from pycoin.serialize import b2h_rev

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.PingPongHandler import PingPongHandler

from pycoinnet.peergroup.BlockChainBuilder import BlockChainBuilder
from pycoinnet.peergroup.ConnectionManager import ConnectionManager
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.util.Queue import Queue

MAINNET_MAGIC_HEADER = binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER = binascii.unhexlify('0B110907')

TESTNET_DNS_BOOTSTRAP = [
    "bitcoin.petertodd.org", "testnet-seed.bitcoin.petertodd.org",
    "bluematt.me", "testnet-seed.bluematt.me"
]

MAINNET_DNS_BOOTSTRAP = [
    "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org"
    "bitseed.xf2.org", "dnsseed.bluematt.me",
]

def queue_of_addresses(magic_header=MAINNET_MAGIC_HEADER, dns_bootstrap=MAINNET_DNS_BOOTSTRAP):
    """
    Returns a queue which is populated with (time, host, port) tuples of
    addresses of regular peers that we can connect to.

    This works by connecting to superpeers at the DNS addresses passed and
    fetching addr records. Once we have enough, stop.
    """

    class GetAddrDelegate:

        def __init__(self, peer, timestamp_address_queue):
            self.getaddr_future = asyncio.Future()
            self.peer = peer
            peer.register_delegate(self)
            self.timestamp_address_queue = timestamp_address_queue

        def handle_msg_addr(self, peer, date_address_tuples, **kwargs):
            for date_address_tuple in date_address_tuples:
                self.timestamp_address_queue.put_nowait(date_address_tuple)

    superpeer_ip_queue = Queue()

    @asyncio.coroutine
    def bootstrap_superpeer_addresses(dns_bootstrap):
        for h in dns_bootstrap:
            r = yield from asyncio.get_event_loop().getaddrinfo(h, 8333)
            results = set(t[-1][:2] for t in r)
            for t in results:
                yield from superpeer_ip_queue.put(t)
                logging.debug("got address %s", t)

    timestamp_address_queue = PriorityQueue()

    @asyncio.coroutine
    def loop_connect_to_superpeer(superpeer_ip_queue):
        while 1:
            try:
                host, port = yield from superpeer_ip_queue.get()
                peer_name = "%s:%d" % (host, port)
                logging.debug("connecting to superpeer at %s:%d", host, port)
                transport, peer = yield from asyncio.get_event_loop().create_connection(
                    lambda: BitcoinPeerProtocol(magic_header), host=host, port=port)
                addr = GetAddrDelegate(peer, timestamp_address_queue)
                peer.run()
                logging.debug("connected to superpeer at %s:%d", host, port)
                asyncio.wait(peer.handshake_complete)
                logging.debug("handshake complete on %s:%d", host, port)
                peer.send_msg("getaddr")
                logging.debug("send getaddr to %s:%d", host, port)
                yield from asyncio.sleep(30)
                logging.debug("closing connection to %s:%d", host, port)
                transport.close()
            except Exception:
                logging.debug("failed during connect to %s:%d", host, port)

    for i in range(30):
        asyncio.Task(loop_connect_to_superpeer(superpeer_ip_queue))

    asyncio.Task(bootstrap_superpeer_addresses(dns_bootstrap))

    return timestamp_address_queue

@asyncio.coroutine
def show(timestamp_address_queue):
    while 1:
        timestamp, addr = yield from timestamp_address_queue.get()
        logging.info("@ %s with address %s", timestamp, addr)
        import pdb; pdb.set_trace()

def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    timestamp_address_queue = queue_of_addresses()
    asyncio.Task(show(timestamp_address_queue))
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
