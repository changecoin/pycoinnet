
import binascii
import logging
import struct

from pycoin.block import Block
from pycoin.tx import script
from pycoin.serialize import b2h_rev

MAINNET_GENESIS = [
    (bytes(reversed(binascii.unhexlify('000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'))), -1, 0)
]


from collections import namedtuple

BlockChainRecord = namedtuple("BlockChainRecord", "hash parent_hash difficulty index_difficulty".split())

def genesis_block_to_block_chain_record(hash, parent_hash=None, difficulty=0, index_difficulty=(-1, 0)):
    return BlockChainRecord(hash, parent_hash, difficulty, index_difficulty)

def block_header_to_block_chain_record(bh):
    return BlockChainRecord(bh.hash(), bh.previous_block_hash, bh.difficulty, index_difficulty=None)

class BlockChain(object):

    """
    @classmethod
    def parse(self, f):
        def h_p_d_iter(f):
            count = struct.unpack("<L", f.read(4))[0]
            p = None
            distance = -1
            total_difficulty = 0
            for i in range(count):
                h = f.read(32)
                difficulty = struct.unpack("<L", f.read(4))[0]
                yield BlockChainRecord(h, p, difficulty, (distance, total_difficulty))
                distance += 1
                total_difficulty += difficulty
                p = h

        the_blockchain = BlockChain()
        items = h_p_d_iter(f)
        the_blockchain.load_records(items)
        return the_blockchain

    def stream(self, f):
        path = self.longest_path
        f.write(struct.pack("<L", len(path)))
        for p in path:
            f.write(p)
            f.write(struct.pack("<L", self.lookup.get(p).difficulty))
    """
    def __init__(self):
        self.childless_hashes = set()
        self.unprocessed_hashes = set()
        self.lookup = {}
        self.next_map = {}
        self._longest_chain_endpoint = None
        self._longest_path = []

    def load_records(self, records_iter):
        # register everything
        for bcr in records_iter:
            self.lookup[bcr.hash] = bcr
            if bcr.index_difficulty:
                self.childless_hashes.add(bcr.hash)
            else:
                self.unprocessed_hashes.add(bcr.hash)
        self.process()

    def record_for_hash(self, hash):
        return self.lookup.get(hash)

    def find_path(self, hash, terminal_condition=lambda bcr: False):
        bcr = self.lookup.get(hash)
        path = [bcr]
        while not terminal_condition(bcr):
            bcr = self.lookup.get(bcr.parent_hash)
            if not bcr:
                break
            path.append(bcr)
        path.reverse()
        return path

    def find_path_to_known_difficulty(self, hash):
        return self.find_path(hash, lambda bcr: bcr.index_difficulty)

    def process(self):
        orphans = set()
        while len(self.unprocessed_hashes) > 0:
            h = self.unprocessed_hashes.pop()
            path = self.find_path_to_known_difficulty(h)
            self.unprocessed_hashes.difference_update(bcr.hash for bcr in path[:-1])
            start_bcr = path[0]
            if not start_bcr.index_difficulty:
                orphans.update(bcr.hash for bcr in path)
                continue

            self.childless_hashes.difference_update(bcr.hash for bcr in path[:-1])
            self.childless_hashes.add(path[-1].hash)
            if start_bcr.index_difficulty:
                distance, total_difficulty = start_bcr.index_difficulty
                for bcr in path[1:]:
                    distance += 1
                    total_difficulty += bcr.difficulty
                    self.lookup[bcr.hash] = bcr._replace(index_difficulty=(distance, total_difficulty))
        self.unprocessed_hashes = orphans
        self._longest_chain_endpoint = max(self.childless_hashes, key=lambda x: self.lookup.get(x).index_difficulty[-1])
        self._longest_path = [bcr.hash for bcr in self.find_path(self._longest_chain_endpoint)]
        self._log()

    def _log(self):
        logging.debug("longest chain endpoint starts with %s and is length %d", b2h_rev(self._longest_path[0]), len(self._longest_path))
        logging.debug("longest chain endpoint ends with %s and is length %d", b2h_rev(self._longest_chain_endpoint), len(self._longest_path))

    def distance(self, h):
        bcr = self.lookup.get(h)
        if bcr:
            return bcr.index_difficulty

    def longest_chain_endpoint(self):
        return self._longest_chain_endpoint

    def longest_path(self):
        return self._longest_path

    def block_by_number(self, index):
        index -= self.lookup[self._longest_path[0]].index_difficulty[0]
        if 0 <= index < len(self._longest_path):
            return self._longest_path[index]
        return None

    def common_ancestor(self, block_hash_1, block_hash_2):
        while 1:
            if block_hash_1 == block_hash_2:
                return block_hash_1
            if self.distance(block_hash_1) > self.distance(block_hash_2):
                block_hash_1, block_hash_2 = block_hash_2, block_hash_1
            bcr2 = self.lookup.get(block_hash_2)
            if not bcr2:
                return None
            block_hash_2 = bcr2.parent_hash
