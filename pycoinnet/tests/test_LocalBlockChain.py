
from pycoinnet.util.LocalBlockChain import LocalBlockChain

"""
Let's make a fake chain 1<=2<=3<=4<=...99<=100
 with branches 3<=3001<=3002<=3003<=3004
"""


def test_1():
    assert 1 == 1


"""
Let's make a fake chain 1<=2<=3<=4<=...99<=100
 with branches 3<=3001<=3002<=3003<=3004
"""

class BHO(object):
    def __init__(self, h, previous_block_hash=None, difficulty=10):
        self.h = h
        self.previous_block_hash = previous_block_hash or h-1
        self.difficulty = difficulty
    def hash(self):
        return self.h
    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)

def test_branch_switch():
    genesis_hash = -1
    lbc = LocalBlockChain(genesis_hash)
    assert lbc.trees_from_bottom == { }
    assert lbc.descendents_by_top == { }
    ITEMS = [BHO(i) for i in range(7)]

    lbc.load_items([ITEMS[0]])
    assert lbc.trees_from_bottom == { 0: [0, -1]}
    assert lbc.descendents_by_top == { -1: {0}}

    lbc.load_items([ITEMS[1]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1]}
    assert lbc.descendents_by_top == { -1: {1}}

    lbc.load_items(ITEMS[0:2])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1]}
    assert lbc.descendents_by_top == { -1: {1}}

    lbc.load_items([ITEMS[4]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 4: [4, 3]}
    assert lbc.descendents_by_top == { -1: {1}, 3: {4}}

    lbc.load_items([ITEMS[3]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 4: [4, 3, 2]}
    assert lbc.descendents_by_top == { -1: {1}, 2: {4}}

    lbc.load_items([ITEMS[5]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 5: [5, 4, 3, 2]}
    assert lbc.descendents_by_top == { -1: {1}, 2: {5}}

    lbc.load_items([ITEMS[2]])
    assert lbc.trees_from_bottom == { 5: [5, 4, 3, 2, 1, 0, -1] }
    assert lbc.descendents_by_top == { -1: {5} }
