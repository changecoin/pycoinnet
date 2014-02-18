import asyncio

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol, BitcoinProtocolError
from pycoinnet.PeerAddress import PeerAddress

MAGIC_HEADER = b"food"


class PeerTransport(asyncio.Transport):
    def __init__(self, write_f, *args, **kwargs):
        super(PeerTransport, self).__init__(*args, **kwargs)
        self.write_f = write_f

    def write(self, data):
        self.write_f(data)

    def close(self):
        pass

    def get_extra_info(self, key):
        class ob:
            def getpeername(self):
                return "192.168.1.1", 8081
        return ob()

def send_version_msg(peer):
    d = peer.default_msg_version_parameters()
    d.update(peer.override_msg_version_parameters)
    peer.send_msg("version", **d)

@asyncio.coroutine
def perform_handshake(peer):
    send_version_msg(peer)
    next_message = peer.new_get_next_message_f()
    peer.start()

    message_name, version_data = yield from next_message()
    if message_name != 'version':
        raise BitcoinProtocolError("missing version")
    peer.send_msg("verack")

    message_name, data = yield from next_message()
    if message_name != 'verack':
        raise BitcoinProtocolError("missing verack")

VERSION_MSG_BIN = b'foodversion\x00\x00\x00\x00\x00^\x00\x00\x00\xe0?\xce\xd8q\x11\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00"\xd7\x03S\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x02\x17\xdf\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x01\x17\xdf\xec\r#\xbb\x82 Z/\t/Notoshi/\x00\x00\x00\x00'

VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=0
)

VERACK_MSG_BIN = b'foodverack\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00]\xf6\xe0\xe2'

def test_BitcoinPeerProtocol_send():
    DATA = []
    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(write_f)
    peer.connection_made(pt)
    assert DATA == []

    peer.send_msg("version", **VERSION_MSG)
    assert DATA[-1] == VERSION_MSG_BIN

    peer.send_msg("verack")
    assert len(DATA) == 2
    assert DATA[-1] == VERACK_MSG_BIN

    peer.send_msg("mempool")
    assert len(DATA) == 3
    assert DATA[-1] == b'foodmempool\x00\x00\x00\x00\x00\x00\x00\x00\x00]\xf6\xe0\xe2'

def test_BitcoinPeerProtocol_read():
    DATA = []
    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(write_f)
    peer.connection_made(pt)

    next_message = peer.new_get_next_message_f()

    @asyncio.coroutine
    def async_test():
        name, data = yield from next_message()
        assert name == 'version'
        assert data == VERSION_MSG
        name, data = yield from next_message()
        assert name == 'verack'
        assert data == {}

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    asyncio.get_event_loop().run_until_complete(async_test())

def test_BitcoinPeerProtocol_multiplex():
    DATA = []
    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(write_f)
    peer.connection_made(pt)

    next_message_list = [peer.new_get_next_message_f() for i in range(50)]

    COUNT = 0
    @asyncio.coroutine
    def async_test(next_message):
        name, data = yield from next_message()
        assert name == 'version'
        assert data == VERSION_MSG
        name, data = yield from next_message()
        assert name == 'verack'
        assert data == {}
        nonlocal COUNT
        COUNT += 1

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    asyncio.get_event_loop().run_until_complete(asyncio.wait([asyncio.Task(async_test(nm)) for nm in next_message_list]))
    assert COUNT == 50


def ztest_BitcoinPeerProtocol():
    def do_test(peer):
        print("send getaddr to %s" % peer)
        yield from perform_handshake(peer)
        peer.send_msg("getaddr")
        
    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2)
    pt2 = PeerTransport(peer1)

    # connect them
    peer1.connection_made(pt2)
    peer2.connection_made(pt1)

    f1 = asyncio.Task(perform_handshake(peer1))
    f2 = asyncio.Task(perform_handshake(peer2))

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))
