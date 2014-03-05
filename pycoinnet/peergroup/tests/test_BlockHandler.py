from pycoinnet.util.debug_help import asyncio

from pycoinnet.peer.tests.helper import create_handshaked_peers, handshake_peers, make_blocks, MAGIC_HEADER, create_peers_tcp
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.peergroup.BlockHandler import BlockHandler

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
#from pycoinnet.peer.HandlerForGet import install_get_handler


def test_BlockHandler_simple():
    # create some peers
    peer1_2, peer2_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.2")
    peer1_3, peer3_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.3")

    BLOCK_LIST = make_blocks(2)

    @asyncio.coroutine
    def run_client(peer_list, block_list):
        block_chain = BlockChain()
        block_store = {}
        inv_collector = InvCollector()
        block_handler = BlockHandler(inv_collector, block_chain, block_store)
        for peer in peer_list:
            inv_collector.add_peer(peer)
            block_handler.add_peer(peer)
        for block in block_list:
            inv_collector.advertise_item(InvItem(ITEM_TYPE_BLOCK, block.hash()))
            block_store[block.hash()] = block
        while len(block_store) < 2:
            yield from asyncio.sleep(0.1)
        return block_store

    f1 = asyncio.Task(run_client([peer1_2, peer1_3], []))
    f2 = asyncio.Task(run_client([peer2_1], BLOCK_LIST[0:1]))
    f3 = asyncio.Task(run_client([peer3_1], BLOCK_LIST[1:2]))

    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2, f3], timeout=5.0))
    assert len(done) == 3
    assert len(pending) == 0
    for i in range(3):
        r = done.pop().result()
        assert len(r) == 2


def make_add_peer(fast_forward_add_peer, blockfetcher, txhandler, inv_collector, block_chain, block_lookup):
    def add_peer(peer, other_last_block_index):
        fast_forward_add_peer(peer, other_last_block_index)
        blockfetcher.add_peer(peer, inv_collector.fetcher_for_peer(peer), other_last_block_index)
        txhandler.add_peer(peer)
        inv_collector.add_peer(peer)
        install_get_handler(peer, block_chain, block_lookup)
    return add_peer


@asyncio.coroutine
def block_getter(inv_q, inv_collector, txhandler, block_chain, block_lookup):
    @asyncio.coroutine
    def fetch_block(inv_item):
        block = yield from inv_collector.fetch(inv_item)
        if block:
            logging.debug("fetched %s", block)
            block_chain.add_headers([block])
            txhandler.add_block(block)
            block_lookup[block.hash()] = block
    while True:
        inv_item = yield from inv_q.get()
        if inv_item is None:
            break
        if inv_item.item_type != ITEM_TYPE_BLOCK:
            continue
        if inv_item.data in block_lookup:
            continue
        asyncio.Task(fetch_block(inv_item))


def items_for_client(initial_blocks=[]):
    block_lookup = dict((b.hash(), b) for b in initial_blocks)

    def is_interested_f(inv_item):
        if inv_item.item_type == ITEM_TYPE_BLOCK:
            return inv_item.data not in block_lookup

    block_chain = BlockChain()
    blockfetcher = Blockfetcher()
    inv_collector = InvCollector()
    txhandler = TxHandler(inv_collector, is_interested_f=lambda inv_item: False)
    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    for block in initial_blocks:
        txhandler.add_block(block)
    block_chain.add_headers(initial_blocks)

    inv_q = inv_collector.new_inv_item_queue()
    asyncio.Task(block_getter(inv_q, inv_collector, txhandler, block_chain, block_lookup))
    ap = make_add_peer(fast_forward_add_peer, blockfetcher, txhandler, inv_collector, block_chain, block_lookup)
    return txhandler, block_chain, block_lookup, ap


def ztest_BlockHandler_tcp():
    BLOCK_LIST = make_blocks(32)
    BL1 = BLOCK_LIST[:-8]
    BL2 = BLOCK_LIST[-8:]

    txhandler_1, block_chain_1, block_lookup_1, add_peer_1 = items_for_client(BL1)
    txhandler_2, block_chain_2, block_lookup_2, add_peer_2 = items_for_client()

    peer1, peer2 = create_peers_tcp()

    handshake_peers(peer1, peer2, dict(local_ip="127.0.0.1", last_block_index=len(BL1)), dict(local_ip="127.0.0.2"))

    add_peer_1(peer1, 0)
    add_peer_2(peer2, len(BL1))

    change_q_1 = block_chain_1.new_change_q()
    change_q_2 = block_chain_2.new_change_q()

    assert block_chain_1.length() == len(BL1)
    assert block_chain_2.length() == 0

    def wait_for_change_q(change_q, count):
        @asyncio.coroutine
        def async_tests(change_q, count):
            r = []
            while len(r) < count:
                v = yield from change_q.get()
                r.append(v)
            return r
        try:
            r = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(async_tests(change_q, count), timeout=5))
        except asyncio.TimeoutError:
            r = []
        return r

    r = wait_for_change_q(change_q_2, len(BL1))

    def show_msgs():
        print('-'*60)
        for m in peer1.msg_list:
            print(m)
        print('-'*60)
        for m in peer2.msg_list:
            print(m)
        print('-'*60)

    assert len(r) == len(BL1)
    assert r == [('add', b.hash(), idx) for idx, b in enumerate(BL1)]

    assert change_q_1.qsize() == 0
    assert change_q_2.qsize() == 0
    assert block_chain_1.length() == len(BL1)
    assert block_chain_2.length() == len(BL1)

    for block in BL2:
        txhandler_1.add_block(block)
        block_lookup_1[block.hash()] = block
    block_chain_1.add_headers(BL2)

    assert block_chain_1.length() == len(BLOCK_LIST)
    assert block_chain_2.length() == len(BL1)

    def wait_for_change_q(change_q, count):
        @asyncio.coroutine
        def async_tests(change_q, count):
            r = []
            while len(r) < count:
                v = yield from change_q.get()
                r.append(v)
            return r
        try:
            r = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(async_tests(change_q, count), timeout=5))
        except asyncio.TimeoutError:
            r = []
        return r

    r = wait_for_change_q(change_q_2, len(BL2))

    show_msgs()

    assert len(r) == len(BL2)
    assert r == [('add', b.hash(), idx+len(BL1)) for idx, b in enumerate(BL2)]

    assert block_chain_1.length() == len(BLOCK_LIST)
    assert block_chain_2.length() == len(BLOCK_LIST)

import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
