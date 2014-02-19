import asyncio

from pycoinnet.helpers import standards
from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol, BitcoinProtocolError
from pycoinnet.PeerAddress import PeerAddress

MAGIC_HEADER = b"food"

class PeerTransport(asyncio.Transport):
    def __init__(self, write_f, peer_name=("192.168.1.1", 8081), *args, **kwargs):
        super(PeerTransport, self).__init__(*args, **kwargs)
        self.write_f = write_f
        self.peer_name = peer_name

    def write(self, data):
        self.write_f(data)

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


def test_initial_handshake():
    @asyncio.coroutine
    def do_test(peer, vp1):
        version_data = yield from standards.initial_handshake(peer, vp1)
        return version_data

    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received, ("127.0.0.1", 8081))
    pt2 = PeerTransport(peer1.data_received, ("127.0.0.2", 8081))

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)

    f1 = asyncio.Task(do_test(peer1, VERSION_MSG))
    f2 = asyncio.Task(do_test(peer2, VERSION_MSG_2))

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))

    assert f1.result() == VERSION_MSG_2
    assert f2.result() == VERSION_MSG
