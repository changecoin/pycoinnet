
"""
Keep track of all connected clients.
"""

import asyncio
import binascii
import logging

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.util.ConnectedClient import ConnectedClient

MAINNET_MAGIC_HEADER=binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER=binascii.unhexlify('0B110907')

class ConnectionManager:

    def __init__(self, address_queue, magic_header=MAINNET_MAGIC_HEADER):
        self.address_queue = address_queue
        self.clients_connecting = set()
        self.clients_connected = set()
        self.magic_header = magic_header
        self.connections = set()

    def run(self, min_connection_count=4):
        self.min_connection_count = min_connection_count
        asyncio.Task(self.keep_minimum_connections())

    @asyncio.coroutine
    def keep_minimum_connections(self):
        connections = set()
        while 1:
            logging.debug("checking connection count (currently %d)", len(connections))
            difference = self.min_connection_count - len(connections)
            for i in range(difference*3):
                asyncio.Task(self.connect_to_remote(asyncio.get_event_loop(), connections))
            yield from asyncio.sleep(10)

    @asyncio.coroutine
    def connect_to_remote(self, event_loop, connections):
        #import pdb; pdb.set_trace()
        host, port = yield from self.address_queue.get()
        #import pdb; pdb.set_trace()
        logging.info("connecting to %s port %d", host, port)
        try:
            transport, protocol = yield from event_loop.create_connection(
                lambda: BitcoinPeerProtocol(self.magic_header),
                host=host, port=port)
            cc = ConnectedClient()
            connections.add(cc)
            yield from cc.run(self, protocol)
            connections.discard(cc)
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
            #address_db.remove_address(host, port)
            #address_db.save()
            return

    """
    def handle_msg_addr(self, client, message):
        self.address_db.add_addresses(
            (timestamp, address.ip_address.exploded, address.port)
            for timestamp, address in message.date_address_tuples)
        self.address_db.save()

    def handle_msg_inv(self, client, message):
        logging.debug("inv : %s", list(message.items))
        items = message.items
        to_fetch = [item for item in items if item.data not in mempool]
        if to_fetch:
            protocol.send_msg_getdata(to_fetch)

    def handle_msg_tx(self, client, message):
        tx = message.tx
        the_hash = tx.hash()
        if not the_hash in mempool:
            mempool[the_hash] = tx
            show_tx(tx)

    def handle_msg_block(self, client, message):
        block = message.block
        the_hash = block.hash()
        if the_hash not in mempool:
            mempool[the_hash] = block
            for tx in block.txs:
                show_tx(tx)

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)
"""
