
from pycoinnet.util.BlockChain import BlockChain


def longest_local_block_chain(self):
    c = []
    for idx in range(self.length()):
        c.append(self.hash_for_index(idx))
    return c


parent_for_0 = "motherless"

def test_basic():
    BC = BlockChain(parent_for_0)
    ITEMS = [(i, i-1, 1) for i in range(100)]
    ITEMS[0] = (0, parent_for_0, 1)

    assert longest_local_block_chain(BC) == []
    assert BC.length() == 0
    assert BC.locked_length() == 0
    assert set(BC.chain_finder.missing_parents()) == set()
    assert BC.parent_hash == parent_for_0
    #assert not BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    assert BC.index_for_hash(0) is None
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[:5])
    assert ops == [("add", i, i) for i in range(5)]
    assert BC.parent_hash == parent_for_0
    assert longest_local_block_chain(BC) == list(range(5))
    assert BC.length() == 5
    assert BC.locked_length() == 0
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0}
    #assert not BC.hash_is_known(-1)
    for i in range(5):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(6)
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[:7])
    assert ops == [("add", i, i) for i in range(5,7)]
    assert BC.parent_hash == parent_for_0
    assert longest_local_block_chain(BC) == list(range(7))
    assert BC.length() == 7
    assert BC.locked_length() == 0
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0}
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(7):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(7)
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[10:14])
    assert ops == []
    assert BC.parent_hash == parent_for_0
    assert longest_local_block_chain(BC) == [0, 1, 2, 3, 4, 5, 6]
    assert BC.locked_length() == 0
    assert BC.locked_length() == 0
    assert BC.length() == 7
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0, 9}
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(7):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(7)
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[7:10])
    assert ops == [("add", i, i) for i in range(7,14)]
    assert longest_local_block_chain(BC) == list(range(14))
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0}
    assert BC.parent_hash == parent_for_0
    assert BC.locked_length() == 0
    assert BC.length() == 14
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(14):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(14)
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[90:])
    assert ops == []
    assert longest_local_block_chain(BC) == list(range(14))
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0, 89}
    assert BC.parent_hash == parent_for_0
    assert BC.locked_length() == 0
    assert BC.length() == 14
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(14):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(14)
    assert BC.index_for_hash(-1) is None

    ops = BC.add_nodes(ITEMS[14:90])
    assert ops == [("add", i, i) for i in range(14,100)]
    assert longest_local_block_chain(BC) == list(range(100))
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0}
    assert BC.parent_hash == parent_for_0
    assert BC.locked_length() == 0
    assert BC.length() == 100
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(100):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(100)
    assert BC.index_for_hash(-1) is None


def test_fork():
    parent_for_0 = b'\0' * 32
    # 0 <= 1 <= ... <= 5 <= 6
    # 3 <= 301 <= 302 <= 303 <= 304 <= 305

    #parent_for_0 = "motherless"
    BC = BlockChain(parent_for_0)
    ITEMS = dict((i, (i, i-1, 1)) for i in range(7))
    ITEMS[0] = (0, parent_for_0, 1)
    

    ITEMS.update(dict((i, (i, i-1, 1)) for i in range(301, 306)))
    ITEMS[301] = (301, 3, 1)

    assert longest_local_block_chain(BC) == []
    assert BC.locked_length() == 0
    assert BC.length() == 0
    assert set(BC.chain_finder.missing_parents()) == set()

    # send them all except 302
    ops = BC.add_nodes((ITEMS[i] for i in ITEMS.keys() if i != 302))
    assert ops == [("add", i, i) for i in range(7)]
    assert set(BC.chain_finder.missing_parents()) == set([parent_for_0, 302])

    # now send 302
    ops = BC.add_nodes([ITEMS[302]])

    # we should see a change
    expected = [("remove", i, i) for i in range(6, 3, -1)]
    expected += [("add", i, i+4-301) for i in range(301,306)]
    assert ops == expected
    assert set(BC.chain_finder.missing_parents()) == set([parent_for_0])


def test_large():
    SIZE = 30000
    ITEMS = [(i, i-1, 1) for i in range(SIZE)]
    ITEMS[0] = (0, parent_for_0, 1)
    BC = BlockChain(parent_for_0)
    assert longest_local_block_chain(BC) == []
    assert BC.locked_length() == 0
    assert BC.length() == 0
    assert set(BC.chain_finder.missing_parents()) == set()

    ops = BC.add_nodes(ITEMS)
    assert ops == [("add", i, i) for i in range(SIZE)]
    assert longest_local_block_chain(BC) == list(range(SIZE))
    assert set(BC.chain_finder.missing_parents()) == {parent_for_0}
    assert BC.parent_hash == parent_for_0
    assert BC.locked_length() == 0
    assert BC.length() == SIZE
    #assert BC.hash_is_known(0)
    #assert not BC.hash_is_known(-1)
    for i in range(SIZE):
        #assert BC.hash_is_known(i)
        v = BC.tuple_for_index(i)
        assert v[0] == i
        assert v[1] == parent_for_0 if i == 0 else i
    #assert not BC.hash_is_known(100)
    assert BC.index_for_hash(-1) is None
