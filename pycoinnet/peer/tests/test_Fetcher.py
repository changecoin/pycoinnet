import asyncio
import hashlib

from pycoin.tx.Tx import Tx, TxIn, TxOut

from pycoinnet.InvItem import InvItem
from pycoinnet.helpers import standards
from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol, BitcoinProtocolError
from pycoinnet.peer.Fetcher import Fetcher
from pycoinnet.PeerAddress import PeerAddress

MAGIC_HEADER = b"food"

class PeerTransport(asyncio.Transport):
    def __init__(self, write_f, peer_name=("192.168.1.1", 8081), *args, **kwargs):
        super(PeerTransport, self).__init__(*args, **kwargs)
        self.write_f = write_f
        self.peer_name = peer_name
        self.writ_data = bytearray()

    def write(self, data):
        self.write_f(data)
        self.writ_data.extend(data)

    def close(self):
        pass

    def get_extra_info(self, key):
        class ob:
            def getpeername(inner_self):
                return self.peer_name
        return ob()


VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=0
)

VERSION_MSG_2 = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760614,
    remote_address=PeerAddress(1, "127.0.0.1", 6111),
    local_address=PeerAddress(1, "127.0.0.2", 6111),
    nonce=5412937754643071,
    last_block_index=0
)


def create_peers():
    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received, ("127.0.0.1", 8081))
    pt2 = PeerTransport(peer1.data_received, ("127.0.0.2", 8081))

    peer1.writ_data = pt1.writ_data
    peer2.writ_data = pt2.writ_data

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)
    return peer1, peer2

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)

def test_fetcher():
    peer1, peer2 = create_peers()

    def make_hash(i):
        return hashlib.sha256(("%d" % i).encode()).digest()

    def make_tx(i):
        txs_in = [TxIn(make_hash(i*10000+idx), (i+idx)%2) for idx in range(3)]
        txs_out = [TxOut(i*40000, make_hash(i*20000+idx)) for idx in range(2)]
        tx = Tx(1, txs_in, txs_out)
        return tx

    TX_LIST = [make_tx(i) for i in range(100)]

    from pycoinnet.message import pack_from_data
    msg_data = pack_from_data("tx", tx=TX_LIST[0])

    msg_data = pack_from_data("getdata", items=[InvItem(1, TX_LIST[0].hash())])


    @asyncio.coroutine
    def run_peer1():
        r = []
        
        yield from standards.initial_handshake(peer1, VERSION_MSG)
        next_message = peer1.new_get_next_message_f()

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("tx", tx=TX_LIST[0])

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("notfound", items=[InvItem(ITEM_TYPE_TX, TX_LIST[1].hash())])

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("tx", tx=TX_LIST[2])

        return r

    @asyncio.coroutine
    def run_peer2():
        r = []
        yield from standards.initial_handshake(peer2, VERSION_MSG_2)
        tx_fetcher = Fetcher(peer2, ITEM_TYPE_TX)

        tx = yield from tx_fetcher.fetch(TX_LIST[0].hash(), timeout=5)
        r.append(tx)

        tx = yield from tx_fetcher.fetch(TX_LIST[1].hash())
        r.append(tx)

        tx = yield from tx_fetcher.fetch(TX_LIST[2].hash())
        r.append(tx)
        return r


    f1 = asyncio.Task(run_peer1())
    f2 = asyncio.Task(run_peer2())

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))

    r = f1.result()
    assert len(r) == 3
    assert r[0] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[0].hash()),)))
    assert r[1] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[1].hash()),)))
    assert r[2] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[2].hash()),)))

    r = f2.result()
    assert len(r) == 3
    assert r[0].hash() == TX_LIST[0].hash()
    assert r[1] == None
    assert r[2].hash() == TX_LIST[2].hash()

import logging
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
