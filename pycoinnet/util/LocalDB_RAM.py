
class LocalDB(object):
    def __init__(self):
        self.lookup = {}

    def add_items(self, items):
        for item in items:
            self.lookup[item.hash()] = item

    def remove_items_with_hash(self, hashes):
        for h in hashes:
            if h in self.lookup:
                del self.lookup[h]

    def hash_is_known(self, h):
        return h in self.lookup

    def item_for_hash(self, h):
        return self.lookup.get(h)

    def all_hashes(self):
        return self.lookup.keys()
