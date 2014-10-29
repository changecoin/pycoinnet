
import asyncio
import logging
import os
import tempfile

from pycoinnet.peergroup.Blockfetcher import Blockfetcher

from pycoinnet.helpers.networks import TESTNET
from pycoinnet.util.tests.helper import make_blocks


LOG_FORMAT = ('%(asctime)s [%(process)d] [%(levelname)s] '
              '%(filename)s:%(lineno)d %(message)s')

def test_get_mined_block():
    # create two clients: A and B
    # create block chain of length 25
    # A connects to B
    # B has 20 blocks
    # A has none
    # A should catch up all 20 blocks
    # A mines a new block
    # B should acquire it from A

    import pdb; pdb.set_trace()

    asyncio.tasks._DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.INFO)

    def do_update(blockchain, ops):
        #import pdb; pdb.set_trace()
        logging.info("update 1: ops=%s", ops)

    client_2_has_20_blocks_future = asyncio.Future()
    client_2_has_25_blocks_future = asyncio.Future()

    def do_update_2(blockchain, ops):
        #import pdb; pdb.set_trace()
        logging.info("update 2: ops=%s", ops)
        if not client_2_has_20_blocks_future.done() and blockchain.length() >= 20:
            client_2_has_20_blocks_future.set_result(blockchain.length())
        if not client_2_has_25_blocks_future.done() and blockchain.length() >= 25:
            client_2_has_25_blocks_future.set_result(blockchain.length())
        #return
        import pdb; pdb.set_trace()
        for op, the_hash, idx in ops:
            if op == 'add':
                block = yield from client_2.get_block(the_hash)
                logging.debug("got block %s" % block.id())

    blocks = make_blocks(25)

    LOOP = asyncio.get_event_loop()

    with tempfile.TemporaryDirectory() as state_dir:
        logging.info("state dir: %s", state_dir)

        host_port_q_1 = asyncio.Queue()
        def should_download_block_f(block_hash, block_index):
            return True
        block_chain_store_1 = BlockChainStore(os.path.join(state_dir, "1"))
        client_1 = Client(TESTNET, should_download_block_f, block_chain_store_1, do_update, host_port_q=host_port_q_1, server_port=9110)

        host_port_q_2 = asyncio.Queue()
        block_chain_store_2 = BlockChainStore(os.path.join(state_dir, "2"))
        client_2 = Client(TESTNET, should_download_block_f, block_chain_store_2, do_update_2, host_port_q=host_port_q_2, server_port=9115)

        def add_blocks_1():
            for b in blocks[:20]:
                client_1.add_block(b)

        def add_blocks_2():
            for b in blocks[20:]:
                client_1.add_block(b)

        def wait():
            LOOP.run_until_complete(asyncio.sleep(0.05))

        LOOP.call_soon(add_blocks_1)

        wait()
        host_port_q_2.put_nowait(("127.0.0.1", 9110))

        wait()
        LOOP.run_until_complete(client_2_has_20_blocks_future)

        wait()

        LOOP.call_soon(add_blocks_2)

        wait()
        LOOP.run_until_complete(client_2_has_25_blocks_future)

