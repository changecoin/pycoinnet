"""
  - takes an InvCollector, BlockChain & BlockStore
  - handle getheaders, getblocks, getdata (for blocks)
  - watch BlockChain
    - on new blocks added, check BlockStore & fetch them
      - useful for fast forward
      - allow BlockStore to determine what we want stored
  - watch InvCollector
    - upon inv, fetch block, call validator, then add to pool
"""

from pycoinnet.util.debug_help import asyncio
import io
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK

def _header_for_block(block):
    f = io.BytesIO()
    block.stream(f)
    f.seek(0)
    return BlockHeader.parse(f)


class BlockHandler:
    def __init__(self, inv_collector, block_chain, block_store,
                 block_validator=lambda block: True, should_download_f=lambda block_hash, block_index: True):
        self.inv_collector = inv_collector
        self.block_store = block_store
        self.q = inv_collector.new_inv_item_queue()
        asyncio.Task(self._watch_invcollector(block_validator))
        #asyncio.Task(self._watch_blockchain(block_chain.new_change_q(), should_download_f))

    @asyncio.coroutine
    def _watch_blockchain(self, change_q, should_download_f):
        # this is only useful when fast-forwarding
        # we will skip it for now
        def _download_block(block_hash, block_index):
            block = yield from self.inv_collector.fetch(InvItem(ITEM_TYPE_BLOCK, block_hash))
        while True:
            add_or_remove, block_hash, block_index = yield from change_q.get()
            if add_or_remove != "add":
                continue
            block = block_store.get(block_hash)
            if block:
                continue
            if should_download_f(block_hash, block_index):
                asyncio.Task(_download_block(block_hash, block_index))

    def _prep_headers(self, hash_stop):
        headers = []
        if hash_stop == b'\0' * 32:
            for i in range(min(2000, self.blockchain.length())):
                h = self.blockchain.hash_for_index(i)
                if h is None:
                    break
                block = self.block_lookup.get(h)
                if block is None:
                    break
                headers.append((_header_for_block(block), 0))
        ## TODO: handle other case where hash_stop != b'\0' * 32
        return headers

    def add_peer(self, peer):
        """
        Call this method when a peer comes online.
        """
        @asyncio.coroutine
        def _run_handle_get(next_message):
            while True:
                name, data = yield from next_message()
                if name == 'getblocks':
                    block_hashes = _prep_block_hashes(blockchain, block_lookup, data.get("hash_stop"))
                    if block_hashes:
                        peer.send_msg("headers", headers=block_hashes)
                if name == 'getheaders':
                    headers = _prep_headers(blockchain, block_lookup, data.get("hash_stop"))
                    if headers:
                        peer.send_msg("headers", headers=headers)
                if name == 'getdata':
                    inv_items = data["items"]
                    not_found = []
                    blocks_found = []
                    for inv_item in inv_items:
                        if inv_item.item_type != ITEM_TYPE_BLOCK:
                            continue
                        block = self.block_store.get(inv_item.data)
                        if block:
                            blocks_found.append(block)
                        else:
                            not_found.append(inv_item)
                    if not_found:
                        peer.send_msg("notfound", items=not_found)
                    for block in blocks_found:
                        peer.send_msg("block", block=block)

        next_message = peer.new_get_next_message_f(lambda name, data: name in ['getheaders', 'getblocks', 'getdata'])
        asyncio.Task(_run_handle_get(next_message))

    @asyncio.coroutine
    def _watch_invcollector(self, block_validator):
        while True:
            inv_item = yield from self.q.get()
            if inv_item.item_type != ITEM_TYPE_BLOCK:
                continue
            self.inv_collector.fetch_validate_store_item_async(inv_item, self.block_store, block_validator)