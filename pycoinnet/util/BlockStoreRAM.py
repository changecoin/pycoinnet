class BlockStoreRAM:
    def __init__(self):
        self.db = {}

    def put_block(self, block):
        self.db[block.hash()] = block

    def get_block_by_hash(self, the_hash):
        return self.db.get(the_hash)

    def get_block_tuple_by_hash(self, the_hash):
        block = self.get_block_by_hash(the_hash)
        if block:
            return (block.hash(), block.previous_block_hash, block.difficulty)

    def all_hashes(self):
        for k in self.db.keys():
            yield k
