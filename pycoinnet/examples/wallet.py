#!/usr/bin/env python

import asyncio
import logging
import os.path

from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.networks import MAINNET
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_pingpong_manager
from pycoinnet.helpers.standards import version_data_for_peer

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.Fetcher import Fetcher

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.BlockHandler import BlockHandler
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.util.BlockChainStore import BlockChainStore
from pycoinnet.util.BlockChain import BlockChain


def storage_base_path():
    p = os.path.expanduser("~/.wallet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


def do_fetch():
    network = MAINNET
    host_port_q = dns_bootstrap_host_port_q(network)

    should_download_block_f = lambda *args, **kwargs: True
    state_dir = storage_base_path()
    block_chain_store = BlockChainStore(state_dir)

    block_chain = BlockChain(did_lock_to_index_f=block_chain_store.did_lock_to_index)

    block_chain.preload_locked_blocks(block_chain_store.headers())

    blockfetcher = Blockfetcher()
    inv_collector = InvCollector()

    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    blockhandler = BlockHandler(inv_collector, block_chain, block_chain_store,
                                should_download_f=should_download_block_f)

    def run_remote():
        change_q = asyncio.Queue()

        def do_update(blockchain, ops):
            from pycoinnet.util.BlockChain import _update_q
            _update_q(change_q, [list(o) for o in ops])

        host, port = yield from host_port_q.get()
        logging.debug("got %s:%d from connection pool", host, port)
        logging.info("connecting to %s:%d" % (host, port))
        try:
            transport, peer = yield from asyncio.get_event_loop().create_connection(
                lambda: BitcoinPeerProtocol(network["MAGIC_HEADER"]), host=host, port=port)
            install_pingpong_manager(peer)
            fetcher = Fetcher(peer)
            logging.info("connected (tcp) to %s:%d", host, port)
            yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
            version_parameters = version_data_for_peer(
                peer, local_port=0, last_block_index=block_chain.length())
            version_data = yield from initial_handshake(peer, version_parameters)
            last_block_index = version_data["last_block_index"]
            print("remote claims block chain of size %d" % last_block_index)
            fast_forward_add_peer(peer, last_block_index)
            blockfetcher.add_peer(peer, fetcher, last_block_index)
            inv_collector.add_peer(peer)
            blockhandler.add_peer(peer)
            block_chain.add_change_callback(do_update)
            while True:
                if block_chain.length() >= last_block_index:
                    break
                while True:
                    op, block_header, index = yield from change_q.get()
                    print("op: %s %s %d" % (op, block_header, index))
                    if change_q.empty():
                        break
            return True
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
        return False

    def connect_to_remote():
        while True:
            is_ok = yield from asyncio.wait_for(run_remote(), timeout=None)
            if is_ok:
                break

    print("At block %d" % block_chain.length())

    task = asyncio.Task(connect_to_remote())

    asyncio.get_event_loop().run_until_complete(task)


def main():
    print(storage_base_path())
    print("wallet. Fetching.")
    do_fetch()


if __name__ == '__main__':
    main()
