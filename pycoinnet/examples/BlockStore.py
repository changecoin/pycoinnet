"""
    - last_block_index
    - hash_by_index
    - block/blockheader_for_hash
    - __contains__ (hash)
"""

import binascii
import os

from pycoin.block import Block
from pycoinnet.util import BlockChain2


class PetrifyError(Exception):
    pass


MAINNET_GENESIS_HASH = bytes(reversed(binascii.unhexlify('000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f')))


from collections import namedtuple

BlockChainRecord = namedtuple("BlockChainRecord", "hash parent_hash difficulty index_difficulty".split())


class BlockStore(object):

    PETRIFIED_FN = "petrified.bin"

    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.petrified_hashes = self._load_petrified_hashes()
        self.petrified_hashes_set = set(self.petrified_hashes)
        self.block_lookup = {}
        self.blockchain = BlockChain2.BlockChain()
        self.blockchain.load_records([BlockChain2.genesis_block_to_block_chain_record(MAINNET_GENESIS_HASH)])
        self.accept_blocks(self._load_blocks(), should_write=False)

    def __contains__(self, h):
        if h in self.petrified_hashes_set:
            return True
        return h in self.block_lookup

    def last_block_index(self):
        bcr = self.blockchain.record_for_hash(self.blockchain.longest_chain_endpoint())
        if bcr:
            return bcr.index_difficulty[0]

    def block_for_hash(self, h):
        return self.block_lookup.get(h)

    def hash_by_index(self, idx):
        if len(self.petrified_hashes) < idx:
            return self.petrified_hashes[idx]
        h = self.blockchain.block_by_number(idx)
        if h:
            return h

    def accept_blocks(self, blocks, should_write=True):
        """
        return new hashes now in the chain if the longest path changed
        """
        for block in blocks:
            if should_write:
                self._write_block(block)
            self.block_lookup[block.hash()] = block
        old_longest_chain_endpoint = self.blockchain.longest_chain_endpoint()
        self.blockchain.load_records(BlockChain2.block_header_to_block_chain_record(block) for block in blocks)
        new_longest_chain_endpoint = self.blockchain.longest_chain_endpoint()
        common_ancestor = self.blockchain.common_ancestor(old_longest_chain_endpoint, new_longest_chain_endpoint)

        new_hashes = []
        k = new_longest_chain_endpoint
        while k and k != common_ancestor:
            new_hashes.append(k)
            k = self.blockchain.record_for_hash(k).parent_hash
        # TODO: should we petrify any?
        return new_hashes

    def petrify(self, count_of_blocks):
        """
        """
        petrify_list = self.blockchain.longest_path[:count_of_blocks]
        if len(petrify_list) < count_of_blocks:
            raise PetrifyError("blockchain does not have enough records")
        if len(self.hashes) > 0:
            bcr = self.blockchain.record_for_hash(blockchain.longest_path[0])
            if not bcr:
                raise PetrifyError("blockchain has no records")
            if self.hashes[-1] != self.blockchain.record_for_hash(blockchain.longest_path[0]).parent_hash:
                raise PetrifyError("blockchain does not extend petrified chain")

        petrify_list = self.blockchain.longest_path[:count_of_blocks]

        # update
        self.petrified_hashes.extend(petrify_list)
        self.petrified_hashes_set.update(petrify_list)

        new_blockchain = BlockChain()
        new_blockchain.load_records(bcr for bcr in self.blockchain.lookup.values() if bcr.hash not in self)
        self.blockchain = new_blockchain

    def _load_petrified_hashes(self):
        def the_hashes(f):
            try:
                while 1:
                    yield f.read(32)
            except Exception:
                pass
        try:
            with file(os.path.join(self.dir_path, self.PETRIFIED_FN), "rb") as f:
                return list(the_hashes(f))
        except Exception:
            return []

    def _petrify_hashes(self, hashes):
        with file(os.path.join(self.dir_path, self.PETRIFIED_FN), "ab") as f:
            for h in self.hashes:
                h1 = bytes(h)
                if len(h1) == 32:
                    f.write(h1)

    def _write_block(self, block):
        with file(os.path.join(self.dir_path, block.id()), "wb") as f:
            block.stream(f)

    def _load_blocks(self):
        paths = os.listdir(self.dir_path)
        for p in paths:
            if re.match(r"[0-9a-f]{64}", p):
                try:
                    with file(os.path.join(self.dir_path, p), "rb") as f:
                        block = Block.parse(f)
                        if block.id() == p:
                            yield block
                except Exception:
                    pass
