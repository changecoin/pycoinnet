
class LocalDB(object):
    def __init__(self):
        self.lookup = {}

    def add_items(self, items):
        for item in items:
            self.lookup[item.hash()] = item
            self._store_item(item)

    def remove_items_with_hash(self, hashes):
        for h in hashes:
            self._remove_item_with_hash(h)
            if h in self.lookup:
                del self.lookup[h]

    def hash_is_known(self, h):
        if h in self.lookup:
            return True
        return self._hash_is_known(h)

    def item_for_hash(self, h):
        if h not in self.lookup:
            self._load_item_for_hash(h)
        return self.lookup.get(h)

    # override the following

    def all_hashes(self):
        return self.lookup.keys()

    def _hash_is_known(self, h):
        return False

    def _store_item(self, item):
        pass

    def _remove_item_with_hash(self, h):
        pass

    def _load_item_for_hash(self, h):
        pass