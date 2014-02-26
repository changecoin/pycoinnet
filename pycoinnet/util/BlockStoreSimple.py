import binascii
import os
import re

from pycoin.block import Block
from pycoin.serialize import b2h_rev


def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))


class BlockStoreSimple:
    def __init__(self, dir_path=None):
        self.dir_path = dir_path

    def _path_for_hash(self, h):
        return os.path.join(self.dir_path, "b_%s" % b2h_rev(h))

    def put_block(self, block):
        with open(self._path_for_hash(block.hash()), "wb") as f:
            block.stream(f)

    def get_block_by_hash(self, the_hash):
        try:
            path = self._path_for_hash(the_hash)
            with open(path, "rb") as f:
                block = Block.parse(f)
                if the_hash == block.hash():
                    return block
        except Exception:
            pass
        return None

    def get_block_tuple_by_hash(self, the_hash):
        block = self.get_block_by_hash(the_hash)
        if block:
            return (block.hash(), block.previous_block_hash, block.difficulty)

    def all_hashes(self):
        paths = os.listdir(self.dir_path)
        paths.sort()
        for p in paths:
            if re.match(r"b_[0-9a-f]{64}", p):
                yield h2b_rev(p)
