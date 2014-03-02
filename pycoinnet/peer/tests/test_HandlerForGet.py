from pycoinnet.util.debug_help import asyncio
#import asyncio
import hashlib

from pycoinnet.helpers import standards
from pycoinnet.peer.HandlerForGet import install_get_handler
from pycoinnet.peer.tests.helper import create_handshaked_peers, make_blocks
from pycoinnet.util.BlockChain import BlockChain

def _header_for_block(block):
    from pycoin.block import BlockHeader
    import io
    f = io.BytesIO()
    block.stream(f)
    f.seek(0)
    return BlockHeader.parse(f)

def test_HandlerForGet_simple_getheader():
    BLOCKS = make_blocks(20)
    blockchain1 = BlockChain()
    blockchain1.add_headers(BLOCKS)

    block_lookup = dict((b.hash(), b) for b in BLOCKS)

    peer1, peer2 = create_handshaked_peers()
    install_get_handler(peer1, blockchain1, block_lookup)

    @asyncio.coroutine
    def run_peer2():
        r = []
        headers = yield from standards.get_headers_hashes(peer2, after_block_hash=b'\0' * 32)
        r.append(headers)
        return r

    f2 = asyncio.Task(run_peer2())

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f2]))

    r = f2.result()
    assert len(r) == 1
    assert [b.hash() for b in r[0]] == [b.hash() for b in BLOCKS]
