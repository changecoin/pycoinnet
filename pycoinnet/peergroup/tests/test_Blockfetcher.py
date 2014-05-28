
import asyncio
import logging
import os
import tempfile

from pycoinnet.examples.Client import Client

from pycoinnet.helpers.networks import TESTNET
from pycoinnet.peer.tests import helper
from pycoinnet.util.BlockChainStore import BlockChainStore


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
        return
        for op, the_hash, idx in ops:
            if op == 'add':
                #block = yield from client_2.get_block(the_hash)
                logging.debug("got block %s" % block.id())
            
    blocks = helper.make_blocks(25)

    with tempfile.TemporaryDirectory() as state_dir:

        host_port_q_1 = asyncio.Queue()
        def should_download_block_f(block_hash, block_index):
            import pdb; pdb.set_trace()
            return True
        block_chain_store_1 = BlockChainStore(os.path.join(state_dir, "1"))
        client_1 = Client(TESTNET, host_port_q_1, should_download_block_f, block_chain_store_1, do_update, server_port=9110)

        host_port_q_2 = asyncio.Queue()
        block_chain_store_2 = BlockChainStore(os.path.join(state_dir, "2"))
        client_2 = Client(TESTNET, host_port_q_2, should_download_block_f, block_chain_store_2, do_update_2, server_port=9115)

        for b in blocks[:20]:
            client_1.add_block(b)

        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(2))
        host_port_q_2.put_nowait(("127.0.0.1", 9110))

        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(5))

        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(2))
        asyncio.get_event_loop().run_until_complete(client_2_has_20_blocks_future)

        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(5))
        for b in blocks[20:]:
            client_1.add_block(b)

        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(5))
        asyncio.get_event_loop().run_until_complete(client_2_has_25_blocks_future)
        #asyncio.get_event_loop().run_until_complete(asyncio.sleep(5))
        #asyncio.get_event_loop().run_forever()


test_get_mined_block()
