import asyncio

from pycoinnet.peer.tests.helper import PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN, create_peers, make_hash, make_tx
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.helpers.standards import initial_handshake

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK


def create_handshaked_peers():
    peer1, peer2 = create_peers()
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, VERSION_MSG), initial_handshake(peer2, VERSION_MSG_2)]))
    return peer1, peer2

def test_InvCollector_simple():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()

    # the peer1a, 1b, 1c represent the local peer

    TX_LIST = [make_tx(i) for i in range(10)]

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        while len(r) < 10:
            inv_item = yield from inv_collector.new_inv_item_queue.get()
            v = yield from inv_collector.fetch(inv_item)
            r.append(v)
        return r

    @asyncio.coroutine
    def run_remote_peer(peer, txs):
        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        while True:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
        return r

    f1 = asyncio.Task(run_local_peer([peer1_2]))
    f2 = asyncio.Task(run_remote_peer(peer2, TX_LIST))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1]))
    r = done.pop().result()
    assert len(r) == 10
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST]


def test_InvCollector():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()
    peer1_3, peer3 = create_handshaked_peers()
    peer1_4, peer4 = create_handshaked_peers()

    # the peer1a, 1b, 1c represent the local peer

    TX_LIST = [make_tx(i) for i in range(100)]

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        while len(r) < 90:
            inv_item = yield from inv_collector.new_inv_item_queue.get()
            v = yield from inv_collector.fetch(inv_item)
            r.append(v)
        return r

    @asyncio.coroutine
    def run_remote_peer(peer, txs):
        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        while True:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
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


def ztest_TxCollector_notfound():
    peer1_2, peer2 = create_handshaked_peers()

    TX_LIST = [make_tx(i) for i in range(10)]

    @asyncio.coroutine
    def run_peer_2(peer, txs):
        # this peer will immediately advertise five transactions
        # But when they are requested, it will say they are "notfound".
        # Then it will sleep 0.25 s, then advertise one transaction,
        # then send it when requested.
        next_message = peer.new_get_next_message_f()

        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs[:5]]
        peer.send_msg("inv", items=inv_items)

        t = yield from next_message()
        r.append(t)
        if t[0] == 'getdata':
            peer.send_msg("notfound", items=t[1]["items"])

        yield from asyncio.sleep(0.25)

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs[5:]]
        peer.send_msg("inv", items=inv_items)

        while 1:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])

        return r

    @asyncio.coroutine
    def run_local_peer(peer_list):
        tx_collector = TxCollector(1.2)
        for peer in peer_list:
            tx_collector.add_peer(peer)
        r = []
        while len(r) < 5:
            v = yield from tx_collector.tx_queue.get()
            r.append(v)
        return r

    f2 = asyncio.Task(run_peer_2(peer2, TX_LIST))

    f = asyncio.Task(run_local_peer([peer1_2]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f], timeout=7.5))
    r = done.pop().result()
    assert len(r) == 5
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST[5:]]


def ztest_TxCollector_retry():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()
    peer1_3, peer3 = create_handshaked_peers()

    TX_LIST = [make_tx(i) for i in range(10)]

    @asyncio.coroutine
    def run_peer_2(peer, txs):
        # this peer will immediately advertise the ten transactions
        # But when they are requested, it will only send one,
        # and "notfound" eight.
        yield from asyncio.sleep(0.4)
        tx_db = dict((tx.hash(), tx) for tx in txs[:1])
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        t = yield from next_message()
        r.append(t)
        if t[0] == 'getdata':
            peer.send_msg("tx", tx=txs[0])
            peer.send_msg("tx", tx=txs[1])
            peer.send_msg("tx", tx=txs[2])
            peer.send_msg("notfound", items=t[1]["items"][3:-3])
        return r

    @asyncio.coroutine
    def run_peer_3(peer, txs):
        # this peer will wait a second, then advertise the ten transactions.

        yield from asyncio.sleep(1.0)

        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        while True:
            next_message = peer.new_get_next_message_f()
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
        return r

    @asyncio.coroutine
    def run_local_peer(peer_list):
        tx_collector = TxCollector(3.0)
        for peer in peer_list:
            tx_collector.add_peer(peer)
        r = []
        while len(r) < 10:
            v = yield from tx_collector.tx_queue.get()
            r.append(v)
        return r

    f2 = asyncio.Task(run_peer_2(peer2, TX_LIST))
    f3 = asyncio.Task(run_peer_3(peer3, TX_LIST))

    f = asyncio.Task(run_local_peer([peer1_2, peer1_3]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f], timeout=7.5))
    assert len(done) == 1
    r = done.pop().result()
    import pdb; pdb.set_trace()
    assert len(r) == 10
    assert set(tx.hash() for tx in r) == set(tx.hash() for tx in TX_LIST)


import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
