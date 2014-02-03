
import binascii
import logging
import struct

from pycoin.block import Block
from pycoin.tx import script
from pycoin.serialize import b2h_rev

#from collections import namedtuple

#BlockChainRecord = namedtuple("BlockChainRecord", "hash parent_hash difficulty".split())

class BlockHashOnly(object):
    __slots__ = "h previous_block_hash difficulty".split()

    def __init__(self, h, previous_block_hash, difficulty):
        self.h = h
        self.previous_block_hash = previous_block_hash
        self.difficulty = difficulty

    def hash(self):
        return self.h
    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)

GENESIS_TRIPLE = (-1, 0, None)

class LocalBlockChain(object):
    def __init__(self):
        self.item_lookup = {}
        self.index_difficulty_lookup = dict()

        self.descendents_by_top = {}
        self.trees_from_bottom = {}

    def __repr__(self):
        return "<LocalBlockChain: trees_fb:%s d_b_tops:%s>" % (self.trees_from_bottom, self.descendents_by_top)

    def load_items(self, items):
        # register everything
        new_hashes = set()
        for item in items:
            h = item.hash()
            if h in self.item_lookup:
                continue
            #item = BlockHashOnly(h, item.previous_block_hash, item.difficulty)
            self.item_lookup[h] = item
            new_hashes.add(h)
        if new_hashes:
            self.meld_new_hashes(new_hashes)

    def meld_new_hashes(self, new_hashes):
        # make a list
        while len(new_hashes) > 0:
            h = new_hashes.pop()
            initial_bottom_h = h
            path = [h]
            item = self.item_lookup.get(h)
            while item:
                h = item.previous_block_hash
                new_hashes.discard(h)
                preceding_path = self.trees_from_bottom.get(h)
                if preceding_path:
                    del self.trees_from_bottom[h]
                    path.extend(preceding_path)
                    # we extended an existing path. Fix up descendents_by_top
                    self.descendents_by_top[preceding_path[-1]].remove(preceding_path[0])
                    break
                path.append(h)
                item = self.item_lookup.get(h)
            self.trees_from_bottom[path[0]] = path

            if len(path) <= 1:
                # this is a lone element... don't bother trying to extend
                return

            # now, perform extensions on any trees that start below here

            bottom_h, top_h = path[0], path[-1]

            top_descendents = self.descendents_by_top.setdefault(top_h, set())
            bottom_descendents = self.descendents_by_top.get(bottom_h)
            if bottom_descendents:
                for descendent in bottom_descendents:
                    prior_path = self.trees_from_bottom[descendent]
                    prior_path.extend(path[1:])
                    if path[0] in self.trees_from_bottom:
                        del self.trees_from_bottom[path[0]]
                    else:
                        pass#import pdb; pdb.set_trace()
                del self.descendents_by_top[bottom_h]
                top_descendents.update(bottom_descendents)
            else:
                top_descendents.add(bottom_h)

    def longest_chain_by_difficulty(self, basis_difficulty_lookup):
        def paths():
            for h, bottoms in self.descendents_by_top.items():
                if h not in basis_difficulty_lookup:
                    continue
                for bottom_h in bottoms:
                    yield bottom_h
        def path_key(bottom_h):
            distance, total_difficulty, basis = self.distance_total_difficulty_basis_triple_for_hash(bottom_h)
            return total_difficulty + basis_difficulty_lookup[basis]
        return max(paths(), key=path_key)
    """
    def longest_chain_by_distance(self, basis_distance_lookup):
        hashes = (h for h in self.descendents_by_top.keys() if h in basis_distance_lookup)
        def chain_length_key(h):
            distance, total_difficulty, basis = self.distance_total_difficulty_basis_triple_for_hash(h)
            return distance + basis_distance_lookup[basis]
        return max(hashes, key=chain_length_key)
    """
    def distance_total_difficulty_basis_triple_for_hash(self, h):
        # is value cached?
        v = self.index_difficulty_lookup.get(h)
        if v:
            # yes. Does the basis now have a parent?
            distance, total_difficulty, basis_hash = v
            item = self.item_lookup.get(basis_hash)
            if item:
                d1, t1, bh1 = self.distance_total_difficulty_basis_triple_for_hash(item.previous_block_hash)
                v = (d1 + distance, t1 + total_difficulty, bh1)
                self.index_difficulty_lookup[h] = v
                return v
            else:
                # we can't improve on the basis
                return v

        item = self.item_lookup.get(h)
        if item:
            distance, total_difficulty, basis_hash = self.distance_total_difficulty_basis_triple_for_hash(item.previous_block_hash)
            v = distance+1, total_difficulty + item.difficulty, basis_hash
        else:
            v = (0, 0, h)
        self.index_difficulty_lookup[h] = v
        return v

    def find_ancestral_path(self, h1, h2):
        p1, p2 = [h1], [h2]
        did_swap = False
        d1, td_1, b1 = self.distance_total_difficulty_basis_triple_for_hash(h1)
        d2, td_2, b2 = self.distance_total_difficulty_basis_triple_for_hash(h2)
        if b1 != b2:
            return [], []
        while 1:
            if h1 == h2:
                if did_swap:
                    return p2, p1
                return p1, p2
            if d1 > d2:
                h1, h2, p1, p2, did_swap = h2, h1, p2, p1, not did_swap
            h2 = self.item_lookup.get(h2).previous_block_hash
            p2.append(h2)
            d2 -= 1
