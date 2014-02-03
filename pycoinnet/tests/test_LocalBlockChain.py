
from pycoinnet.util.LocalBlockChain import LocalBlockChain

"""
Let's make a fake chain 1<=2<=3<=4<=...99<=100
 with branches 3<=3001<=3002<=3003<=3004
"""

class BHO(object):
    def __init__(self, h, previous_block_hash=None, difficulty=10):
        self.h = h
        self.previous_block_hash = h-1 if previous_block_hash is None else previous_block_hash
        self.difficulty = difficulty
    def hash(self):
        return self.h
    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)

def do_scramble(items, tfb, dbt):
    import itertools
    for c in itertools.permutations(items):
        lbc = LocalBlockChain()
        lbc.load_items(c)
        assert lbc.trees_from_bottom == tfb
        assert lbc.descendents_by_top == dbt
        lbc = LocalBlockChain()
        for b in c:
            lbc.load_items([b])
        assert lbc.trees_from_bottom == tfb
        assert lbc.descendents_by_top == dbt

def test_basics():
    lbc = LocalBlockChain()
    assert lbc.trees_from_bottom == { }
    assert lbc.descendents_by_top == { }
    ITEMS = [BHO(i) for i in range(6)]

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

    do_scramble(ITEMS, lbc.trees_from_bottom, lbc.descendents_by_top)

def test_branch():
    lbc = LocalBlockChain()
    assert lbc.trees_from_bottom == { }
    assert lbc.descendents_by_top == { }
    ITEMS = [BHO(i) for i in range(7)]
    B301 = BHO(301, 3, 10)
    B302, B303, B304 = [BHO(i) for i in range(302,305)]

    lbc.load_items([B302])
    assert lbc.trees_from_bottom == { 302: [302, 301]}
    assert lbc.descendents_by_top == { 301: {302}}

    lbc.load_items([B304])
    assert lbc.trees_from_bottom == { 302: [302, 301], 304: [304, 303]}
    assert lbc.descendents_by_top == { 301: {302}, 303: {304}}

    lbc.load_items([B303])
    assert lbc.trees_from_bottom == { 304: [304, 303, 302, 301] }
    assert lbc.descendents_by_top == { 301: {304} }

    lbc.load_items(ITEMS)
    assert lbc.trees_from_bottom == { 6: [6, 5, 4, 3, 2, 1, 0, -1], 304: [304, 303, 302, 301] }
    assert lbc.descendents_by_top == { -1: {6}, 301: {304} }

    lbc.load_items([B301])
    assert lbc.trees_from_bottom == { 6: [6, 5, 4, 3, 2, 1, 0, -1], 304: [304, 303, 302, 301, 3, 2, 1, 0, -1] }
    assert lbc.descendents_by_top == { -1: {6, 304} }

def test_0123():
    I0 = BHO(0)
    I1 = BHO(1)
    I2 = BHO(2)
    I3 = BHO(3, 1)
    lbc = LocalBlockChain()
    lbc.load_items([I0, I2, I3, I1])
    assert lbc.trees_from_bottom == { 2: [2, 1, 0, -1], 3: [3, 1, 0, -1] }
    assert lbc.descendents_by_top == { -1: {2,3} }

def test_all_orphans():
    I0 = BHO(0)
    I1 = BHO(1)
    I2 = BHO(2)
    I3 = BHO(3)
    lbc = LocalBlockChain()
    lbc.load_items([I2, I3, I1])
    assert lbc.trees_from_bottom == { 3: [3, 2, 1, 0] }
    assert lbc.descendents_by_top == { 0: {3} }

def test_scramble():
    ITEMS = [BHO(i, (i-1)//2, 10) for i in range(7)]
    tfb = { 3: [3, 1, 0, -1], 4: [4,1,0,-1], 5: [5,2,0,-1], 6:[6,2,0,-1] }
    dbt = { -1: {3,4,5,6}}
    do_scramble(ITEMS, tfb, dbt)

def test_branch_switch():
    lbc = LocalBlockChain()
    assert lbc.trees_from_bottom == { }
    assert lbc.descendents_by_top == { }
    ITEMS = [BHO(i) for i in range(4)]
    B201 = BHO(201, 2, 10)
    B202, B203, B204 = [BHO(i) for i in range(202,205)]

    items = ITEMS + [B201, B202, B203, B204]
    tfb = { 204: [204, 203, 202, 201, 2, 1, 0, -1], 3:[3, 2, 1, 0, -1]}
    dbt = { -1: {3, 204}}
    do_scramble(items, tfb, dbt)

def test_longest_chain_endpoint():
    lbc = LocalBlockChain()
    ITEMS = [BHO(i) for i in range(5)]
    B201 = BHO(201, 2, 110)
    B202, B203, B204 = [BHO(i) for i in range(202,205)]

    items = ITEMS + [B201, B202, B203, B204]
    tfb = { 204: [204, 203, 202, 201, 2, 1, 0, -1], 4:[4, 3, 2, 1, 0, -1]}
    dbt = { -1: {3, 204}}
    lbc.load_items(items)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(0) == (1, 10, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(1) == (2, 20, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(2) == (3, 30, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(3) == (4, 40, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(4) == (5, 50, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(201) == (4, 140, -1)
    assert lbc.distance_total_difficulty_basis_triple_for_hash(202) == (5, 150, -1)

    lbc = LocalBlockChain()
    items = ITEMS + [B202, B203, B204]
    lbc.load_items(items)

    old_chain_endpoint = lbc.longest_chain_by_difficulty({ -1: 0 })
    assert old_chain_endpoint == 4

    lbc.load_items([B201])

    new_chain_endpoint = lbc.longest_chain_by_difficulty({ -1: 0 })
    assert new_chain_endpoint == 204

    old_subpath, new_subpath = lbc.find_ancestral_path(old_chain_endpoint, new_chain_endpoint)
    assert old_subpath == [4, 3, 2]
    assert new_subpath == [204, 203, 202, 201, 2]

    old_subpath, new_subpath = lbc.find_ancestral_path(3, new_chain_endpoint)
    assert old_subpath == [3, 2]
    assert new_subpath == [204, 203, 202, 201, 2]

    old_subpath, new_subpath = lbc.find_ancestral_path(2, new_chain_endpoint)
    assert old_subpath == [2]
    assert new_subpath == [204, 203, 202, 201, 2]
