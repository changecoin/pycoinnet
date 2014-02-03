
from pycoinnet.examples.BlockChain import BlockChain
from pycoinnet.util.PetrifyDB_RAM import PetrifyDB
from pycoinnet.util.LocalDB_RAM import LocalDB

class BHO(object):
    def __init__(self, h, previous_block_hash=None, difficulty=10):
        self.h = h
        self.previous_block_hash = h-1 if previous_block_hash is None else previous_block_hash
        self.difficulty = difficulty
    def hash(self):
        return self.h
    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)

def test_basic():
    parent_for_0 = "motherless"
    petrify_db = PetrifyDB(parent_for_0)
    local_db = LocalDB()
    BC = BlockChain(local_db, petrify_db)
    ITEMS = [BHO(i) for i in range(100)]
    ITEMS[0].previous_block_hash = parent_for_0

    assert BC.longest_local_block_chain() == []
    assert BC.longest_local_block_chain_length() == 0
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(0) == False
    assert BC.hash_is_known(-1) == False
    assert BC.item_for_hash(0) == None
    assert BC.item_for_hash(-1) == None

    new_hashes, old_hashes = BC.add_items(ITEMS[:5])
    assert new_hashes == [4, 3, 2, 1]
    assert old_hashes == []
    assert BC.petrified_block_count() == 0
    assert BC.longest_local_block_chain() == [4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 5
    assert BC.block_chain_size() == 5
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(-1) == False
    for i in range(5):
        assert BC.hash_is_known(i) == True
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i==0 else i
    assert BC.hash_is_known(6) == False
    assert BC.item_for_hash(-1) == None

    new_hashes, old_hashes = BC.add_items(ITEMS[:7])
    assert new_hashes == [6, 5]
    assert old_hashes == []
    assert BC.petrified_block_count() == 0
    assert BC.longest_local_block_chain() == [6, 5, 4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 7
    assert BC.block_chain_size() == 7
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(0) == True
    assert BC.hash_is_known(-1) == False
    for i in range(7):
        assert BC.hash_is_known(i) == True
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i==0 else i
    assert BC.hash_is_known(7) == False
    assert BC.item_for_hash(-1) == None

    new_hashes, old_hashes = BC.add_items(ITEMS[10:14])
    assert new_hashes == []
    assert old_hashes == []
    assert BC.petrified_block_count() == 0
    assert BC.petrified_block_count() == 0
    assert BC.longest_local_block_chain() == [6, 5, 4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 7
    assert BC.block_chain_size() == 7
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(0) == True
    assert BC.hash_is_known(-1) == False
    for i in range(7):
        assert BC.hash_is_known(i) == True
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i==0 else i
    assert BC.hash_is_known(7) == False
    assert BC.item_for_hash(-1) == None

    new_hashes, old_hashes = BC.add_items(ITEMS[7:10])
    assert new_hashes == [13, 12, 11, 10, 9, 8, 7]
    assert old_hashes == []
    assert BC.longest_local_block_chain() == [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3]
    assert BC.longest_local_block_chain_length() == 11
    assert BC.petrified_block_count() == 4
    assert BC.block_chain_size() == 14
    assert BC.hash_is_known(0) == True
    assert BC.hash_is_known(-1) == False
    for i in range(14):
        assert BC.hash_is_known(i) == True
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i==0 else i
    assert BC.hash_is_known(14) == False
    assert BC.item_for_hash(-1) == None

    #import pdb; pdb.set_trace()


    """
    def longest_local_block_chain(self):
    def longest_local_block_chain_length(self):
    def block_chain_size(self):
    def petrified_block_count(self):
    def block_chain_size(self):
    def hash_is_known(self, h):
    def hash_for_index(self, idx):
    def item_for_hash(self, h):
    def add_items(self, items):
    def petrify_blocks(self, to_petrify_count):
    """
