import re
import os

from pycoin.serialize import b2h_rev
from pycoinnet.util import LocalBlockChain


class LocalDB(object):
    def __init__(self, dir_path):
        self.dir_path = dir_path

    def add_items(self, items):
        for item in items:
            with open(os.path.join(self.dir_path, item.id()), "wb") as f:
                item.stream(f)

    def remove_items_with_hash(self, hashes):
        for h in hashes:
            self._remove_block(h)

    def hash_is_known(self, h):
        return self.item_for_hash(h) != None

    def item_for_hash(self, h):
        p = b2h_rev(h)
        try:
            path = os.path.join(self.dir_path, p)
            with open(path, "rb") as f:
                block = BlockHeader.parse(f)
                if block.id() == p:
                    yield block
        except Exception:
            pass

    def _remove_block(self, block_hash):
        path = os.path.join(self.dir_path, b2h_rev(block_hash))
        try:
            os.remove(path)
        except FileNotFoundError:
            logging.info("missing %s already", path)

    def all_hashes(self):
        paths = os.listdir(self.dir_path)
        for p in paths:
            if re.match(r"[0-9a-f]{64}", p):
                yield p
