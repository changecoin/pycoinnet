import binascii
import logging
import re
import os

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev

from .LocalDB_RAM import LocalDB


def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))


class LocalDB(LocalDB):
    def __init__(self, dir_path):
        super(LocalDB, self).__init__()
        self.dir_path = dir_path

    def all_hashes(self):
        paths = os.listdir(self.dir_path)
        for p in paths:
            if re.match(r"[0-9a-f]{64}", p):
                yield h2b_rev(p)

    def _store_item(self, item):
        with open(os.path.join(self.dir_path, item.id()), "wb") as f:
            item.stream(f)

    def _remove_item_with_hash(self, block_hash):
        path = os.path.join(self.dir_path, b2h_rev(block_hash))
        try:
            os.remove(path)
        except FileNotFoundError:
            logging.info("missing %s already", path)

    def _load_item_for_hash(self, h):
        p = b2h_rev(h)
        try:
            path = os.path.join(self.dir_path, p)
            with open(path, "rb") as f:
                block = BlockHeader.parse(f)
                if block.id() == p:
                    self.lookup[block.hash()] = block
        except Exception:
            pass

