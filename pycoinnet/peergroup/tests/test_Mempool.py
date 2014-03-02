import asyncio

from pycoinnet.peer.tests.helper import create_handshaked_peers, make_tx, make_block
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.peergroup.Mempool import Mempool

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK


def test_Mempool_simple():
    # create some peers
    peer1_2, peer2_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.2")
    peer1_3, peer3_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.3")

    TX_LIST = [make_tx(i) for i in range(20)]
    BLOCK_LIST = [make_block(i) for i in range(2)]

    @asyncio.coroutine
    def run_client(peer_list, tx_list, block_list):
        inv_collector = InvCollector()
        mempool = Mempool(inv_collector)
        for tx in tx_list:
            mempool.add_tx(tx)
        for block in block_list:
            mempool.add_block(block)
        for peer in peer_list:
            inv_collector.add_peer(peer)
            mempool.add_peer(peer)
        for peer in peer_list:
            peer.send_msg("mempool")
        while len(mempool.tx_pool) < 20 and len(mempool.block_pool) < 2:
            yield from asyncio.sleep(0.1)
        return mempool.tx_pool

    f1 = asyncio.Task(run_client([peer1_2, peer1_3], [], []))
    f2 = asyncio.Task(run_client([peer2_1], TX_LIST[:10], BLOCK_LIST[0:1]))
    f3 = asyncio.Task(run_client([peer3_1], TX_LIST[10:], BLOCK_LIST[1:2]))

    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2, f3], timeout=5.0))
    assert len(done) == 3
    assert len(pending) == 0
    for i in range(3):
        r = done.pop().result()
        assert len(r) == 20
        assert set(tx.hash() for tx in r.values()) == set(tx.hash() for tx in TX_LIST)





def test_Mempool_tcp():
    # create server

    PORT = 60661

    TX_LIST = [make_tx(i) for i in range(20)]
    BLOCK_LIST = make_blocks(10)

    block_lookup = dict((b.hash(), b) for b in BLOCK_LIST)

    def create_peer1():
        block_chain = BlockChain()
        change_q = block_chain.new_change_q()

        blockfetcher = Blockfetcher()
        inv_collector = InvCollector()
        mempool = Mempool(inv_collector)

        BL1 = BLOCK_LIST[:-1]
        BL2 = BLOCK_LIST[-1:]
        for block in BL1:
            mempool.add_block(block)

        block_chain.add_nodes(
            (block.hash(), block.previous_block_hash, block.difficulty) for block in BL1)

        fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

        @asyncio.coroutine
        def run_peer1(peer):
            yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
            version_parameters = default_msg_version_parameters(peer, last_block_index=block_chain.length())
            version_data = yield from initial_handshake(peer, version_parameters)
            last_block_index = version_data["last_block_index"]
            fast_forward_add_peer(peer, last_block_index)
            blockfetcher.add_peer(peer, inv_collector.fetcher_for_peer(peer), last_block_index)
            mempool.add_peer(peer)
            inv_collector.add_peer(peer)
            install_get_handler(peer, block_chain, block_lookup)
            yield from asyncio.sleep(3)
            import pdb; pdb.set_trace()

        peer = BitcoinPeerProtocol(MAGIC_HEADER)
        asyncio.Task(run_peer1(peer))
        return peer

    def create_peer2():
        block_chain = BlockChain()
        change_q = block_chain.new_change_q()

        blockfetcher = Blockfetcher()
        inv_collector = InvCollector()
        mempool = Mempool(inv_collector)

        fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

        @asyncio.coroutine
        def run_peer2(peer):
            yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
            version_parameters = default_msg_version_parameters(peer, last_block_index=block_chain.length())
            version_data = yield from initial_handshake(peer, version_parameters)
            last_block_index = version_data["last_block_index"]
            fast_forward_add_peer(peer, last_block_index)
            blockfetcher.add_peer(peer, inv_collector.fetcher_for_peer(peer), last_block_index)
            mempool.add_peer(peer)
            inv_collector.add_peer(peer)
            install_get_handler(peer, block_chain, block_lookup)
            yield from asyncio.sleep(3)
            ops = yield from change_q.get()
            import pdb; pdb.set_trace()

        peer = BitcoinPeerProtocol(MAGIC_HEADER)
        asyncio.Task(run_peer2(peer))
        return peer

    @asyncio.coroutine
    def run_listener():
        abstract_server = yield from asyncio.get_event_loop().create_server(protocol_factory=create_peer1, port=PORT)
        return abstract_server

    @asyncio.coroutine
    def run_connector():
        transport, protocol = yield from asyncio.get_event_loop().create_connection(
            create_peer2, host="127.0.0.1", port=PORT)

    asyncio.Task(run_listener())
    asyncio.Task(run_connector())

    asyncio.get_event_loop().run_forever()


import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
