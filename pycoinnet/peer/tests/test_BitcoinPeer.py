import asyncio

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol, BitcoinProtocolError

MAGIC_HEADER = b"food"


class PeerTransport(asyncio.Transport):
    def __init__(self, remote_peer, *args, **kwargs):
        super(PeerTransport, self).__init__(*args, **kwargs)
        self.remote_peer = remote_peer

    def write(self, data):
        self.remote_peer.data_received(data)

    def close(self):
        pass

    def get_extra_info(self, key):
        class ob:
            def getpeername(self):
                return "192.168.1.1", 8081
        return ob()

def test_BitcoinPeerProtocol():
    def do_test():
        peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
        peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

        pt1 = PeerTransport(peer2)
        pt2 = PeerTransport(peer1)

        # connect them
        peer1.connection_made(pt2)
        peer2.connection_made(pt1)

        yield from asyncio.wait_for(peer1.handshake_complete, timeout=None)
        yield from asyncio.wait_for(peer2.handshake_complete, timeout=None)

        peer1.send_msg("getaddr")
        peer2.send_msg("getaddr")

        

    asyncio.get_event_loop().run_until_complete(do_test())