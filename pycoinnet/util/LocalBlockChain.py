
import binascii
import logging
import struct

from pycoin.block import Block
from pycoin.tx import script
from pycoin.serialize import b2h_rev

from collections import namedtuple

BlockChainRecord = namedtuple("BlockChainRecord", "hash parent_hash difficulty index_difficulty".split())

def genesis_block_to_block_chain_record(hash, parent_hash=None, difficulty=0, index_difficulty=(-1, 0)):
    return BlockChainRecord(hash, parent_hash, difficulty, index_difficulty)

def block_header_to_block_chain_record(bh):
    return BlockChainRecord(bh.hash(), bh.previous_block_hash, bh.difficulty, index_difficulty=None)

class LocalBlockChain(object):
    def __init__(self, petrify_db=None):
        self.childless_hashes = set()
        self.unprocessed_hashes = set()
        self.petrify_db = petrify_db
        self.lookup = {}
        self._longest_chain_endpoint = None
        self._longest_path = []
        genesis_hash = petrify_db.last_hash()
        bcr = BlockChainRecord(genesis_hash, None, 0, index_difficulty=(-1,0))
        import pdb; pdb.set_trace()
        self.load_records([bcr])

    def load_records(self, records_iter):
        # register everything
        for bcr in records_iter:
            if (not bcr.index_difficulty) and self.petrify_db and self.petrify_db.hash_is_known(bcr.hash):
                continue
            self.lookup[bcr.hash] = bcr
            if bcr.index_difficulty:
                self.childless_hashes.add(bcr.hash)
            else:
                self.unprocessed_hashes.add(bcr.hash)
        self.process()

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

    def find_path_to_known_difficulty(self, hash, known_orphans):
        return self.find_path(hash, lambda bcr: bcr.index_difficulty or bcr.hash in known_orphans)

    def process(self):
        orphans = set()
        while len(self.unprocessed_hashes) > 0:
            h = self.unprocessed_hashes.pop()
            path = self.find_path_to_known_difficulty(h, orphans)
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

    def hash_by_number(self, index):
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
