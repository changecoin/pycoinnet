"""
Handle getheaders and getblocks messages.
"""

from pycoinnet.util.debug_help import asyncio
import io
import logging

from pycoin.block import BlockHeader, Block

def _header_for_block(block):
    f = io.BytesIO()
    block.stream(f)
    f.seek(0)
    return BlockHeader.parse(f)

def _prep_headers(blockchain, block_lookup, hash_stop):
    headers = []
    if hash_stop == b'\0' * 32:
        for i in range(min(2000, blockchain.length())):
            h = blockchain.hash_for_index(i)
            if h is None:
                break
            block = block_lookup.get(h)
            if block is None:
                break
            headers.append((_header_for_block(block), 0))
    return headers

def install_get_handler(peer, blockchain, block_lookup):
    @asyncio.coroutine
    def _run(peer, blockchain, block_lookup, next_message):
        try:
            while True:
                name, data = yield from next_message()
                if name == 'getheaders':
                    headers = _prep_headers(blockchain, block_lookup, data.get("hash_stop"))
                    peer.send_msg("headers", headers=headers)
                if name == 'getblocks':
                    import pdb; pdb.set_trace()
        except EOFError:
            pass

    next_message = peer.new_get_next_message_f(lambda name, data: name in ['getheaders', 'getblocks'])
    asyncio.Task(_run(peer, blockchain, block_lookup, next_message))

