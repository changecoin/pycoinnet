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


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class ConnectedClient(object):

    def __init__(self, controller=None):
        self.controller = controller

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
        pass

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
        message = yield from self.protocol.next_message()
        if message.name != 'version':
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

        while self.is_running:
            message = yield from self.protocol.next_message()

            # handle pongs here
            if message.name == 'pong':
                logging.debug("got pong %s", message.nonce)
                ping_nonces.discard(message.nonce)

            if message.name == 'ping':
                logging.debug("got ping %s", message.nonce)
                self.protocol.send_msg_pong(message.nonce)

            handler_name = "handle_msg_%s" % message.name

            if hasattr(self.controller, handler_name):
                controller_handler = getattr(self.controller, handler_name)
                controller_handler(self, message)
