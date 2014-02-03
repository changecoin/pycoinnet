
from pycoinnet.util.LocalBlockChain import LocalBlockChain

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
        load_items(lbc, c)
        assert lbc.trees_from_bottom == tfb
        assert lbc.descendents_by_top == dbt
        lbc = LocalBlockChain()
        for b in c:
            load_items(lbc, [b])
        assert lbc.trees_from_bottom == tfb
        assert lbc.descendents_by_top == dbt

def load_items(lbc, bhos):
    return lbc.load_nodes((bh.h, bh.previous_block_hash) for bh in bhos)

def test_basics():
    lbc = LocalBlockChain()
    assert lbc.trees_from_bottom == { }
    assert lbc.descendents_by_top == { }
    ITEMS = [BHO(i) for i in range(6)]

    load_items(lbc, [ITEMS[0]])
    assert lbc.trees_from_bottom == { 0: [0, -1]}
    assert lbc.descendents_by_top == { -1: {0}}

    load_items(lbc, [ITEMS[1]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1]}
    assert lbc.descendents_by_top == { -1: {1}}

    load_items(lbc, ITEMS[0:2])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1]}
    assert lbc.descendents_by_top == { -1: {1}}

    load_items(lbc, [ITEMS[4]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 4: [4, 3]}
    assert lbc.descendents_by_top == { -1: {1}, 3: {4}}

    load_items(lbc, [ITEMS[3]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 4: [4, 3, 2]}
    assert lbc.descendents_by_top == { -1: {1}, 2: {4}}

    load_items(lbc, [ITEMS[5]])
    assert lbc.trees_from_bottom == { 1: [1, 0, -1], 5: [5, 4, 3, 2]}
    assert lbc.descendents_by_top == { -1: {1}, 2: {5}}

    load_items(lbc, [ITEMS[2]])
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

    load_items(lbc, [B302])
    assert lbc.trees_from_bottom == { 302: [302, 301]}
    assert lbc.descendents_by_top == { 301: {302}}

    load_items(lbc, [B304])
    assert lbc.trees_from_bottom == { 302: [302, 301], 304: [304, 303]}
    assert lbc.descendents_by_top == { 301: {302}, 303: {304}}

    load_items(lbc, [B303])
    assert lbc.trees_from_bottom == { 304: [304, 303, 302, 301] }
    assert lbc.descendents_by_top == { 301: {304} }

    load_items(lbc, ITEMS)
    assert lbc.trees_from_bottom == { 6: [6, 5, 4, 3, 2, 1, 0, -1], 304: [304, 303, 302, 301] }
    assert lbc.descendents_by_top == { -1: {6}, 301: {304} }

    load_items(lbc, [B301])
    assert lbc.trees_from_bottom == { 6: [6, 5, 4, 3, 2, 1, 0, -1], 304: [304, 303, 302, 301, 3, 2, 1, 0, -1] }
    assert lbc.descendents_by_top == { -1: {6, 304} }

def test_0123():
    I0 = BHO(0)
    I1 = BHO(1)
    I2 = BHO(2)
    I3 = BHO(3, 1)
    lbc = LocalBlockChain()
    load_items(lbc, [I0, I2, I3, I1])
    assert lbc.trees_from_bottom == { 2: [2, 1, 0, -1], 3: [3, 1, 0, -1] }
    assert lbc.descendents_by_top == { -1: {2,3} }

def test_all_orphans():
    I0 = BHO(0)
    I1 = BHO(1)
    I2 = BHO(2)
    I3 = BHO(3)
    lbc = LocalBlockChain()
    load_items(lbc, [I2, I3, I1])
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

    def node_weight_f(h):
        if h == -1:
            return 0
        if h == 201:
            return 110
        return 10

    items = ITEMS + [B201, B202, B203, B204]
    load_items(lbc, items)
    assert lbc.difficulty(0, node_weight_f) == 10
    assert lbc.difficulty(1, node_weight_f) == 20
    assert lbc.difficulty(2, node_weight_f) == 30
    assert lbc.difficulty(3, node_weight_f) == 40
    assert lbc.difficulty(4, node_weight_f) == 50
    assert lbc.difficulty(201, node_weight_f) == 140
    assert lbc.difficulty(202, node_weight_f) == 150


def test_find_ancestral_path():

    ITEMS = [BHO(i) for i in range(5)]
    B201 = BHO(201, 2, 110)
    B202, B203, B204 = [BHO(i) for i in range(202,205)]

    lbc = LocalBlockChain()
    items = ITEMS + [B202, B203, B204]
    load_items(lbc, items)

    old_longest_chains = lbc.longest_chains()
    assert old_longest_chains == [[4, 3, 2, 1, 0, -1]]

    load_items(lbc, [B201])

    new_longest_chains = lbc.longest_chains()
    for l in [204, 203, 202, 201, 2, 1, 0, -1], [4, 3, 2, 1, 0, -1]:
        assert l in new_longest_chains
    assert len(new_longest_chains) == 2

    old_chain_endpoint, new_chain_endpoint = 4, 204

    old_subpath, new_subpath = lbc.find_ancestral_path(old_chain_endpoint, new_chain_endpoint)
    assert old_subpath == [4, 3, 2]
    assert new_subpath == [204, 203, 202, 201, 2]
