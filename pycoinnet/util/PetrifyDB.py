
import os

from pycoin.serialize import b2h_rev


class BlockHashOnly(object):
    def __init__(self, h, previous_block_hash, difficulty):
        self.h = h
        self.previous_block_hash = previous_block_hash
        self.difficulty = difficulty

    def hash(self):
        return self.h


class PetrifyError(Exception):
    pass


class PetrifyDB(object):
    PETRIFIED_FN = "petrified.bin"

    def __init__(self, dir_path, genesis_hash):
        self.dir_path = dir_path
        self.genesis_hash = genesis_hash
        self.petrified_hashes = self._load_petrified_hashes()
        self.petrified_hashes_lookup = dict((v,k) for k,v in enumerate(self.petrified_hashes))

    def _log(self):
        logging.debug("petrified chain is length %d", len(self.petrified_hashes))
        if len(self.petrified_hashes):
            logging.debug("petrified chain starts with %s", b2h_rev(self.petrified_hashes[0]))
            logging.debug("petrified chain ends with %s", b2h_rev(self.petrified_hashes[-1]))
        if len(self.petrified_hashes_lookup) < len(self.petrified_hashes):
            logging.error("warning: petrified_hashes_lookup has %d members", len(self.petrified_hashes_lookup))

    def index_for_hash(self, h):
        if h == self.genesis_hash:
            return -1
        return self.petrified_hashes_lookup.get(h)

    def hash_for_index(self, idx):
        if idx < 0:
            return self.genesis_hash
        if 0 <= idx < len(self.petrified_hashes):
            return self.petrified_hashes[idx]

    def item_for_hash(self, h):
        return self.item_for_index(self.index_for_hash(h))

    def item_for_index(self, idx):
        if idx is None or idx < 0:
            return BlockHashOnly(h=self.genesis_hash, previous_block_hash=b'\0'*32, difficulty=0)
        return BlockHashOnly(h=self.hash_for_index(idx), previous_block_hash=self.hash_for_index(idx-1), difficulty=0)

    def hash_is_known(self, h):
        return self.item_for_hash(h) != None

    def count_of_hashes(self):
        return len(self.petrified_hashes)

    def all_hashes(self):
        return self.petrified_hashes

    def last_hash(self):
        if self.petrified_hashes:
            return self.petrified_hashes[-1]
        return self.genesis_hash

    def add_chain(self, items):
        last_hash = self.last_hash()
        if items.hash() != last_hash:
            raise PetrifyError("blockchain does not extend petrified chain (expecting %s)" % b2h_rev(last_hash))
        self._petrify_hashes(items)

    def _load_petrified_hashes(self):
        def the_hashes(f):
            try:
                while 1:
                    d = f.read(16384)
                    if len(d) == 0:
                        return
                    while len(d) >= 32:
                        yield d[:32]
                        d = d[32:]
            except Exception:
                pass
        try:
            with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "rb") as f:
                return list(the_hashes(f))
        except Exception:
            return []

    def _petrify_hashes(self, items):
        with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "ab") as f:
            for item in items:
                h1 = item.hash()
                if len(h1) == 32:
                    f.write(h1)
