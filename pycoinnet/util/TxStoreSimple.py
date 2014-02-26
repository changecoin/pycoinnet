import binascii
import os
import re

from pycoin.tx.Tx import Tx
from pycoin.serialize import b2h_rev


def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))


class TxStoreSimple:
    def __init__(self, dir_path=None):
        self.dir_path = dir_path

    def _path_for_hash(self, h):
        return os.path.join(self.dir_path, "tx_%s" % b2h_rev(h))

    def put_tx(self, tx):
        with open(self._path_for_hash(tx.hash()), "wb") as f:
            tx.stream(f)

    def get_tx_by_hash(self, the_hash):
        try:
            path = self._path_for_hash(the_hash)
            with open(path, "rb") as f:
                tx = Tx.parse(f)
                if the_hash == tx.hash():
                    return tx
        except Exception:
            pass
        return None

    def all_hashes(self):
        paths = os.listdir(self.dir_path)
        paths.sort()
        for p in paths:
            if re.match(r"tx_[0-9a-f]{64}", p):
                yield h2b_rev(p)
