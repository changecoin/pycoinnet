#!/usr/bin/env python

"""
This bitcoin client does little more than try to keep an up-to-date
list of available clients in a text file "addresses".
"""

import asyncio
import logging
import os
import struct
import time

from pycoinnet.message import MESSAGE_STRUCTURES

from pycoinnet.InvItem import InvItem
from pycoinnet.util.Queue import Queue

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class BitcoinPeer(object):

    def __init__(self):
        self.inv_items_requested = Queue()
        self.inv_item_futures = {}
        self.delegate_methods = dict((msg_name, []) for msg_name in MESSAGE_STRUCTURES.keys())
        self.register_delegate(self)

    def register_delegate(self, delegate):
        for msg_name in MESSAGE_STRUCTURES.keys():
            method_name = "handle_msg_%s" % msg_name
            if hasattr(delegate, method_name):
                self.delegate_methods[msg_name].append(getattr(delegate, method_name))

    def unregister_delegate(self, delegate):
        for msg_name in MESSAGE_STRUCTURES.keys():
            method_name = "handle_msg_%s" % msg_name
            if hasattr(delegate, method_name):
                self.delegate_methods[msg_name].remove(getattr(delegate, method_name))

    def get_msg_version_parameters(self, transport):
        # this must return a dictionary with:
        #  version (integer)
        #  subversion (bytes, like b"/Satoshi:0.7.2/")
        #  services (a mask, set to 1 for now)
        #  current time (seconds since epoch)
        #  remote_address
        #  remote_listen_port
        #  local_address
        #  local_listen_port
        #  nonce (32 bit)
        #  last_block_index
        #  want_relay
        remote_address, remote_port = transport.get_extra_info("socket").getpeername()
        return dict(
            version=70001,
            subversion=b"/Notoshi/",
            services=1,
            current_time=int(time.time()),
            remote_address=remote_address,
            remote_listen_port=remote_port,
            local_address="127.0.0.1",
            local_listen_port=6111,
            nonce=struct.unpack("!Q", os.urandom(8))[0],
            last_block_index=0,
            want_relay=True
        )

    def did_complete_handshake(self):
        #self.protocol.send_msg_getaddr()
        self.protocol.send_msg_mempool()

    @asyncio.coroutine
    def run(self, connection_manager, protocol):
        self.connection_manager = connection_manager
        self.protocol = protocol

        self.heartbeat_rate = 60
        self.missing_pong_disconnect_timeout = 60

        try:
            yield from self.do_handshake()
            self.did_complete_handshake()
            yield from self.run_until_complete()
        except Exception:
            logging.exception("closing connection")
        self.protocol.transport.close()

    @asyncio.coroutine
    def do_handshake(self):
        d = self.get_msg_version_parameters(self.protocol.transport)
        self.protocol.send_msg_version(**d)
        message_name, data = yield from self.protocol.next_message()
        if message_name != 'version':
            raise Exception("missing version")
        self.protocol.send_msg_verack()

    @asyncio.coroutine
    def run_until_complete(self):
        # set to false for an orderly disconnect
        self.is_running = True

        # this block handles pings
        ping_nonces = set()
        def ping_heartbeat():
            while True:
                now = time.time()
                if self.protocol.last_message_timestamp + self.heartbeat_rate < now:
                    # we need to ping!
                    nonce = struct.unpack("!Q", os.urandom(8))[0]
                    self.protocol.send_msg_ping(nonce)
                    logging.debug("sending ping %d", nonce)
                    ping_nonces.add(nonce)
                    yield from asyncio.sleep(self.missing_pong_disconnect_timeout)
                    if nonce in ping_nonces:
                        # gotta hang up!
                        self.is_running = False
                        ## BRAIN DAMAGE: how do we get out of this loop?
                        ## it seems we will hang on protocol.next_message?
                yield from asyncio.sleep(self.protocol.last_message_timestamp + self.heartbeat_rate - now)
        asyncio.Task(ping_heartbeat())
        asyncio.Task(self.process_inv_items_requested())

        while self.is_running:
            message_name, data = yield from self.protocol.next_message()
            if message_name in self.delegate_methods:
                methods = self.delegate_methods.get(message_name)
                if methods:
                    for m in methods:
                        #import pdb; pdb.set_trace()
                        m(self, **data)
            else:
                logging.error("unknown message %s", message_name)

    def handle_msg_pong(self, peer, nonce, **kwargs):
        logging.debug("got pong %s", nonce)
        ping_nonces.discard(nonce)

    def handle_msg_ping(self, peer, nonce, **kwargs):
        logging.debug("got ping %s", nonce)
        peer.protocol.send_msg_pong(nonce)

    def handle_msg_tx(self, peer, tx, **kwargs):
        inv_item = InvItem(ITEM_TYPE_TX, tx.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            if not future.done():
                future.set_result(tx)
            else:
                logging.info("got %s unsolicited", tx.id())

    def handle_msg_block(self, peer, block, **kwargs):
        inv_item = InvItem(ITEM_TYPE_BLOCK, block.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            if not future.done():
                future.set_result(block)
            else:
                logging.info("got %s unsolicited", block.id())

    @asyncio.coroutine
    def request_inv_item(self, inv_item, timeout=15):
        """timeout is naive... we want to be smarter about it somehow"""
        future = asyncio.Future()
        yield from self.inv_items_requested.put((inv_item, future))
        done, pending = yield from asyncio.wait([future], timeout=timeout)
        if len(done) > 0:
            exc = future.exception()
            if exc: raise exc
            return future.result()
        for p in pending:
            p.cancel()
        return None

    @asyncio.coroutine
    def process_inv_items_requested(self):
        while True:
            pairs = yield from self.inv_items_requested.get_all()
            while len(pairs) > 0:
                for inv_item, future in pairs:
                    self.inv_item_futures[inv_item] = future
                self.protocol.send_msg_getdata([p[0] for p in pairs[:50000]])
                pairs = pairs[50000:]

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.protocol.transport.get_extra_info("socket").getpeername())
