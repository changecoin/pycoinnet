class TxStoreRAM:
    def __init__(self):
        self.db = {}

    def put_tx(self, tx):
        self.db[tx.hash()] = tx

    def get_tx_by_hash(self, the_hash):
        return self.db.get(the_hash)

    def all_hashes(self):
        for k in self.db.keys():
            yield k
