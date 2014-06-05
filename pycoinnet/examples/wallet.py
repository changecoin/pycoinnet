#!/usr/bin/env python

import asyncio
import os.path

from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.networks import MAINNET
from pycoinnet.examples.Client import Client
from pycoinnet.util.BlockChainStore import BlockChainStore


def storage_base_path():
    p = os.path.expanduser("~/.wallet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


def do_fetch():

    IS_LOOPING = False

    def blockchain_change_callback(blockchain, ops):
        if not IS_LOOPING:
            return
        for op, the_block, block_index in ops:
            print(op, the_block, block_index)

    network = MAINNET

    host_port_q = dns_bootstrap_host_port_q(network)
    should_download_block_f = lambda *args, **kwargs: True
    state_dir = storage_base_path()
    block_chain_store = BlockChainStore(state_dir)
    client = Client(network, host_port_q, should_download_block_f, block_chain_store,
                    blockchain_change_callback, server_port=None)

    chain_length = client.blockchain_length()
    print("At block %d" % chain_length)

    IS_LOOPING = True

    asyncio.get_event_loop().run_forever()


def main():
    print(storage_base_path())
    print("wallet. Fetching.")
    do_fetch()


if __name__ == '__main__':
    main()
