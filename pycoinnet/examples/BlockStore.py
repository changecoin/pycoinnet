"""
    - last_block_index
    - hash_by_index
    - block/blockheader_for_hash
    - __contains__ (hash)
"""

import binascii
import logging
import os
import re

from pycoin.block import Block, BlockHeader
from pycoin.serialize import b2h_rev
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
        logging.debug("petrified chain is length %d", len(self.petrified_hashes))
        if len(self.petrified_hashes):
            logging.debug("petrified chain starts with %s", b2h_rev(self.petrified_hashes[0]))
            logging.debug("petrified chain ends with %s", b2h_rev(self.petrified_hashes[-1]))
        self.block_lookup = {}
        self.blockchain = BlockChain2.BlockChain()
        genesis = self.petrified_hashes[-1] if len(self.petrified_hashes) else MAINNET_GENESIS_HASH
        logging.debug("genesis petrified hash is %s", b2h_rev(genesis))
        self.blockchain.load_records([BlockChain2.genesis_block_to_block_chain_record(genesis)])
        self.accept_blocks(self._load_blocks(), should_write=False)

    def __contains__(self, h):
        if h in self.petrified_hashes_set:
            return True
        return h in self.block_lookup

    def last_block_index(self):
        bcr = self.blockchain.record_for_hash(self.blockchain.longest_chain_endpoint())
        if bcr:
            return bcr.index_difficulty[0] + len(self.petrified_hashes)

    def last_block_hash(self):
        return self.blockchain.longest_chain_endpoint()

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

        def block_iter(blocks):
            for block in blocks:
                if block.hash() in self.petrified_hashes_set:
                    continue
                if should_write:
                    self._write_block(block)
                self.block_lookup[block.hash()] = block
                yield block
        old_longest_chain_endpoint = self.blockchain.longest_chain_endpoint()
        self.blockchain.load_records(BlockChain2.block_header_to_block_chain_record(block) for block in block_iter(blocks))
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
        longest_path = self.blockchain.longest_path()
        if len(self.petrified_hashes) == 0:
            petrify_list = longest_path[:count_of_blocks]
        else:

            petrify_list = longest_path[1:1+count_of_blocks]
            bcr = self.blockchain.record_for_hash(longest_path[0])
            if not bcr:
                raise PetrifyError("blockchain has no records")
            if self.petrified_hashes[-1] != bcr.parent_hash:
                raise PetrifyError("blockchain does not extend petrified chain")

        if len(petrify_list) < count_of_blocks:
            raise PetrifyError("blockchain does not have enough records")

        self._petrify_hashes(petrify_list)

        # update
        self.petrified_hashes.extend(petrify_list)
        self.petrified_hashes_set.update(petrify_list)

        new_blockchain = BlockChain2.BlockChain()
        new_blockchain.load_records(longest_path[:1])
        new_blockchain.load_records(bcr for bcr in self.blockchain.lookup.values() if bcr.hash not in self.petrified_hashes_set)
        self.blockchain = new_blockchain

    def longest_nonpetrified_chain(self):
        return self.blockchain.longest_path()

    def _load_petrified_hashes(self):
        def the_hashes(f):
            try:
                while 1:
                    d = f.read(16384)
                    if len(d) == 0: return
                    while len(d) > 32:
                        yield d[:32]
                        d = d[32:]
            except Exception:
                pass
        try:
            with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "rb") as f:
                return list(the_hashes(f))
        except Exception:
            return []

    def _petrify_hashes(self, hashes):
        with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "ab") as f:
            for h in hashes:
                h1 = bytes(h)
                if len(h1) == 32:
                    f.write(h1)
                    self._remove_block(h1)

    def _remove_block(self, block_hash):
        path = os.path.join(self.dir_path, b2h_rev(block_hash))
        try:
            os.remove(path)
        except FileNotFoundError:
            logging.info("missing %s already", path)

    def _write_block(self, block):
        with open(os.path.join(self.dir_path, block.id()), "wb") as f:
            block.stream(f)

    def _load_blocks(self):
        paths = os.listdir(self.dir_path)
        for p in paths:
            if re.match(r"[0-9a-f]{64}", p):
                try:
                    path = os.path.join(self.dir_path, p)
                    with open(path, "rb") as f:
                        block = BlockHeader.parse(f)
                        if block.hash() in self.petrified_hashes_set:
                            self._remove_block(block.hash())
                            continue
                        if block.id() == p:
                            yield block
                except Exception:
                    pass
