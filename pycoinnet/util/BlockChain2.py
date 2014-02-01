
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
        return self._longest_path[index]

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

    def process_longest_chain(self, block_lookup):
        while 1:
            block_chain = self.calculate_longest_chain()
            tx_db = {}
            opening_txs = {}
            for i, block_hash in enumerate(block_chain):
                if block_hash == BLOCK_0_HASH:
                    continue
                logging.debug("block %d [%s]", i, b2h_rev(block_hash))
                block = block_lookup(block_hash)
                if block is None:
                    logging.error("missing block for %s", b2h_rev(block_hash))
                    self.unregister_block(block_hash)
                    break
                if not self.validate_block_txs(block, tx_db, opening_txs):
                    logging.error("failed validation in block %s", b2h_rev(block_hash))
                    self.unregister_block(block_hash)
                    break
            else:
                return block_chain
            logging.debug("recalculating longest chain")

    def validate_block_txs(self, block, tx_db, opening_txs):
        for i, tx in enumerate(block.txs):
            if i>0:
                logging.debug("  tx %d [%s]", i, tx)
            tx_hash = tx.hash()
            tx_db[tx_hash] = tx
            for j, tx_in in enumerate(tx.txs_in):
                if tx_in.previous_hash != '\0' * 32 or j != 0:
                    tx_prev = tx_db.get(tx_in.previous_hash)
                    v = script.verify_signature(tx_prev, tx, j)
                    logging.debug("verifying signature for %s: %s", tx_in, "OK" if v else "** BAD")
                    if not v:
                        logging.error("bad signature in %s [%d]", b2h_rev(tx_hash), j)
                        return False
                opening_txs[(tx_in.previous_hash, tx_in.previous_index)] = (tx_in, j)
            for j, tx_out in enumerate(tx.txs_out):
                opening_txs[(tx_hash, j)] = None
        return True
