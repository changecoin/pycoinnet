import asyncio

from pycoinnet.peer.tests.helper import PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN, create_peers, make_hash, make_tx
from pycoinnet.peergroup.TxCollector import TxCollector
from pycoinnet.helpers.standards import initial_handshake

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK


def create_handshaked_peers():
    peer1, peer2 = create_peers()
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, VERSION_MSG), initial_handshake(peer2, VERSION_MSG_2)]))
    return peer1, peer2

def test_TxCollector():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()
    peer1_3, peer3 = create_handshaked_peers()
    peer1_4, peer4 = create_handshaked_peers()

    # the peer1a, 1b, 1c represent the same process

    TX_LIST = [make_tx(i) for i in range(100)]

    @asyncio.coroutine
    def run_remote_peer(peer, txs):
        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        while True:
            t = yield from next_message()
            if t == None:
                break
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
        return r

    @asyncio.coroutine
    def run_local_peer(peer_list):
        tx_collector = TxCollector()
        for peer in peer_list:
            tx_collector.add_peer(peer)
        r = []
        while len(r) < 90:
            v = yield from tx_collector.tx_queue.get()
            r.append(v)
        return r

    futures = []
    for peer, txs in [(peer2, TX_LIST[:30]), (peer3, TX_LIST[30:60]), (peer4, TX_LIST[60:90])]:
        f = asyncio.Task(run_remote_peer(peer, txs))
        futures.append(f)

    f = asyncio.Task(run_local_peer([peer1_2, peer1_3, peer1_4]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f]))
    r = done.pop().result()
    assert len(r) == 90
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST[:90]]

import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
