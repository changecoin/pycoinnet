
"""
Keep track of all connected peers.
"""

import asyncio.queues
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.InvItem import InvItem
from pycoinnet.util.BitcoinPeer import BitcoinPeer

MAINNET_MAGIC_HEADER=binascii.unhexlify('F9BEB4D9')
TESTNET_MAGIC_HEADER=binascii.unhexlify('0B110907')

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

class ConnectionManager:

    def __init__(self, address_queue, magic_header=MAINNET_MAGIC_HEADER):
        self.address_queue = address_queue
        self.peers_connecting = set()
        self.peers_connected = set()
        self.magic_header = magic_header
        self.connections = set()

        self.inv_item_queue = asyncio.queues.Queue()
        self.inv_futures = {}

    def run(self, min_connection_count=4):
        self.min_connection_count = min_connection_count
        asyncio.Task(self.keep_minimum_connections())
        asyncio.Task(self.collect_inventory_loop())

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
        host, port = yield from self.address_queue.get()
        logging.info("connecting to %s port %d", host, port)
        try:
            transport, protocol = yield from event_loop.create_connection(
                lambda: BitcoinPeerProtocol(self.magic_header),
                host=host, port=port)
            peer = BitcoinPeer(self)
            connections.add(peer)
            yield from peer.run(self, protocol)
            connections.discard(peer)
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
            #address_db.remove_address(host, port)
            #address_db.save()
            return

    ### inventory collector

    def handle_msg_inv(self, peer, message):
        logging.debug("inv from %s : %s", peer, list(message.items))
        self.inv_item_queue.put_nowait((peer, message))

    def handle_msg_tx(self, peer, message):
        tx = message.tx
        item = InvItem(ITEM_TYPE_TX, tx.hash())
        import pdb; pdb.set_trace()
        if item not in self.inv_futures:
            logging.error("got tx %s but it was never requested", b2h_rev(tx.hash()))
            return
        self.inv_futures[item].set_result(tx)
        ## TODO: put tx into a queue where it can be pulled out and dealt with
        show_tx(tx)

    def handle_msg_block(self, peer, message):
        block = message.block
        item = InvItem(ITEM_TYPE_BLOCK, block.hash())
        if item not in self.inv_futures:
            logging.error("got block %s but it was never requested", b2h_rev(block.hash()))
            return
        self.inv_futures[item].set_result(block)
        ## TODO: put block into a queue where it can be pulled out and dealt with

    @asyncio.coroutine
    def collect_inventory_loop(self):

        peers_with_inv_item = {}

        while True:
            peer, message = yield from self.inv_item_queue.get()
            logging.debug("collect_inventory_loop peer %s message %s", peer, message)
            items = message.items
            for item in items:
                # don't get Tx items for now
                #if item.type == ITEM_TYPE_TX:
                #    continue
                logging.debug("peer %s has inv item %s", peer, item)
                if item not in peers_with_inv_item:
                    peers_with_inv_item[item] = set()
                peer_set = peers_with_inv_item[item]
                peer_set.add(peer)
                peer_set_nonempty = asyncio.Event()
                peer_set_nonempty.set()
                if not item in self.inv_futures:
                    logging.debug("initiating download_inv_item for item %s", item)
                    def done_callback(future):
                        import pdb; pdb.set_trace()
                        logging.debug("in done_callback for item %s", item)
                        del peers_with_inv_item[item]
                    self.inv_futures[item] = asyncio.Task(self.download_inv_item(item, peer_set, peer_set_nonempty))
                    self.inv_futures[item].add_done_callback(done_callback)

    @asyncio.coroutine
    def download_inv_item(self, item, peer_set, peer_set_nonempty, peer_timeout=15):
        peers_tried = set()
        peers_we_can_try = set()
        while True:
            more_peers = peer_set_nonempty.is_set()
            if more_peers:
                peers_we_can_try.update(peer_set)
                peer_set.clear()
                peer_set_nonempty.clear()

            peers_we_can_try.difference_update(peers_tried)
            if len(peers_we_can_try) == 0:
                yield from peer_set_nonempty.wait()
                continue

            # pick a peer. For now, we just do it randomly
            peer = peers_we_can_try.pop()
            peers_tried.add(peer)

            logging.debug("trying to fetch %s from %s", item, peer)
            # BRAIN DAMAGE: queue it up in the peer so it can potentially handle multiple
            future = peer.protocol.send_msg_getdata([item])
            done, pending = yield from asyncio.wait([future], timeout=peer_timeout)
            import pdb; pdb.set_trace()
            if len(done) > 0:
                break
            for p in pending:
                p.cancel()
            # and try another

    ### end inventory collector


    """
    def handle_msg_addr(self, peer, message):
        self.address_db.add_addresses(
            (timestamp, address.ip_address.exploded, address.port)
            for timestamp, address in message.date_address_tuples)
        self.address_db.save()

    def handle_msg_tx(self, peer, message):
        tx = message.tx
        the_hash = tx.hash()
        if not the_hash in mempool:
            mempool[the_hash] = tx
            show_tx(tx)

    def handle_msg_block(self, peer, message):
        block = message.block
        the_hash = block.hash()
        if the_hash not in mempool:
            mempool[the_hash] = block
            for tx in block.txs:
                show_tx(tx)
"""

def show_tx(tx):
    logging.info("Tx ID %s", b2h_rev(tx.hash()))
    for idx, tx_out in enumerate(tx.txs_out):
        ba = tx_out.bitcoin_address()
        if ba:
            logging.info("%d: %s %s BTC", idx, ba, satoshi_to_btc(tx_out.coin_value))
        else:
            logging.info("can't figure out destination of tx_out id %d", idx)
