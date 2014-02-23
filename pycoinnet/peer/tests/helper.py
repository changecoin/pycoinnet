import asyncio
import hashlib

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.PeerAddress import PeerAddress

from pycoin.tx.Tx import Tx, TxIn, TxOut

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


VERSION_MSG_BIN = b'foodversion\x00\x00\x00\x00\x00^\x00\x00\x00\xe0?\xce\xd8q\x11\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00"\xd7\x03S\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x02\x17\xdf\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x01\x17\xdf\xec\r#\xbb\x82 Z/\t/Notoshi/\x00\x00\x00\x00'

VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=0
)

VERACK_MSG_BIN = b'foodverack\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00]\xf6\xe0\xe2'

VERSION_MSG_2 = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760614,
    remote_address=PeerAddress(1, "127.0.0.1", 6111),
    local_address=PeerAddress(1, "127.0.0.2", 6111),
    nonce=5412937754643071,
    last_block_index=0
)

def create_peers(ip1="127.0.0.1", ip2="127.0.0.2"):
    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received, (ip2, 6111))
    pt2 = PeerTransport(peer1.data_received, (ip1, 6111))

    peer1.writ_data = pt1.writ_data
    peer2.writ_data = pt2.writ_data

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)
    return peer1, peer2

def create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.2"):
    peer1, peer2 = create_peers(ip1, ip2)
    pa1 = PeerAddress(1, ip1, 6111)
    pa2 = PeerAddress(1, ip2, 6111)
    msg1 = dict(VERSION_MSG)
    msg1.update(dict(local_address=pa1, remote_address=pa2))
    msg2 = dict(VERSION_MSG)
    msg2.update(dict(local_address=pa2, remote_address=pa1))
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, msg1), initial_handshake(peer2, msg2)]))
    return peer1, peer2

def make_hash(i):
    return hashlib.sha256(("%d" % i).encode()).digest()

def make_tx(i):
    txs_in = [TxIn(make_hash(i*10000+idx), (i+idx)%2) for idx in range(3)]
    txs_out = [TxOut(i*40000, make_hash(i*20000+idx)) for idx in range(2)]
    tx = Tx(1, txs_in, txs_out)
    return tx
