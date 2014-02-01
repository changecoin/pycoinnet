
import binascii
import logging
import struct

from pycoin.block import Block
from pycoin.tx import script
from pycoin.serialize import b2h_rev

MAINNET_GENESIS = [
    (bytes(reversed(binascii.unhexlify('000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'))), -1, 0)
]

class BlockChain(object):

    @classmethod
    def parse(self, f):
        def h_p_d_iter(f):
            count = struct.unpack("<L", f.read(4))[0]
            p = None
            for i in range(count):
                h = f.read(32)
                difficulty = struct.unpack("<Q", f.read(8))[0]
                yield h, p, difficulty
                p = h

        items = h_p_d_iter(f)
        h, p, difficulty = next(items)
        the_blockchain = BlockChain(genesis=[(h, -1, difficulty)])
        the_blockchain.load_items(items)
        return the_blockchain

    def stream(self, f):
        path = self.longest_chain()
        f.write(struct.pack("<L", len(path)))
        for p in path:
            f.write(p)
            f.write(struct.pack("<Q", self.difficulty_map.get(p)))

    def __init__(self, genesis=MAINNET_GENESIS):
        self.prev_map = {}
        self.difficulty_map = dict((k, v2) for k, v1, v2 in genesis)
        self.distance_map = dict((k, (v1, v2)) for k, v1, v2 in genesis)
        self.maximal_chain_endpoints = set(k for k, v1, v2 in genesis)
        self.unprocessed_block_hashes = set()
        self.block_marker_frequency = 100
        self.next_map = {}

    def load_blocks(self, block_iter):
        self.load_items((b.hash(), b.previous_block_hash, b.difficulty) for b in block_iter)

    def load_items(self, items_iter):
        # register everything
        for h, previous_block_hash, difficulty in items_iter:
            self.prev_map[h] = previous_block_hash
            self.difficulty_map[h] = difficulty
            self.unprocessed_block_hashes.add(h)
        self.process()

    def distance(self, block_hash, local_distance_map={}):
        k = block_hash
        path = [k]
        # walk backwards
        while k in self.prev_map and k not in self.distance_map and k not in local_distance_map:
            k = self.prev_map[k]
            path.append(k)
        if k in self.distance_map or k in local_distance_map:
            # we have info to populate these items!
            difficulty, block_number = self.distance_map.get(k, local_distance_map.get(k))
            logging.info("processing path of length %d", len(path))
            self.maximal_chain_endpoints.discard(path[-1])
            self.maximal_chain_endpoints.add(path[0])
            while len(path) > 0:
                k = path.pop()
                difficulty += self.difficulty_map[k]
                local_distance_map[k] = (difficulty, block_number)
                block_number += 1
        if block_hash in local_distance_map:
            return local_distance_map[block_hash]
        return self.distance_map.get(block_hash)

    def common_ancestor(self, block_hash_1, block_hash_2):
        local_distance_map = {}
        while 1:
            if block_hash_1 == block_hash_2:
                return block_hash_1
            d1 = self.distance(block_hash_1, local_distance_map)
            d2 = self.distance(block_hash_2, local_distance_map)
            if d1[0] > d2[0]:
                block_hash_1 = self.prev_map.get(block_hash_1)
            else:
                block_hash_2 = self.prev_map.get(block_hash_2)
            if None in [block_hash_1, block_hash_2]:
                return None

    def block_number(self, index):
        local_distance_map = {}
        h = self.longest_chain_endpoint()
        d1 = self.distance(h, local_distance_map)
        if d1[1] < index:
            return None
        while h:
            d1 = self.distance(h, local_distance_map)
            if d1[1] == index:
                return h
            h = self.prev_map.get(h)
        return None

    def process(self):
        local_distance_map = {}
        while len(self.unprocessed_block_hashes) > 0:
            k = self.unprocessed_block_hashes.pop()
            if k in self.next_map:
                self.unprocessed_block_hashes.update(self.next_map[k])
                del self.next_map[k]
                continue
            if k in self.distance_map or k in local_distance_map:
                continue
            distance = self.distance(k, local_distance_map)
            if not distance:
                # we need to mark this as free, to be handled when its parent shows up
                if k not in self.next_map:
                    self.next_map[k] = set()
                self.next_map[k].add(path[0])

        for k in self.maximal_chain_endpoints:
            self.distance_map[k] = local_distance_map[k]

        self.distance_map.update(
            (k,(difficulty, block_number)) for k,(difficulty, block_number)
                in self.distance_map.items()
                    if block_number % self.block_marker_frequency == 0)

    def register_block(self, block):
        self.load_blocks([block])

    def longest_chain_endpoint(self):
        h, (difficulty, block_number) = max((x for x in self.distance_map.items() if x[0] in self.maximal_chain_endpoints), key=lambda x: x[1][0])
        return h

    def longest_chain(self):
        h = self.longest_chain_endpoint()
        path = [h]
        while h in self.prev_map:
            h = self.prev_map[h]
            path.append(h)
        path.reverse()
        return path

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
