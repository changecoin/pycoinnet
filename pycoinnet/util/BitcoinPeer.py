import asyncio
import logging
import os
import struct
import time

from pycoinnet.message import MESSAGE_STRUCTURES

ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class BitcoinPeer(object):

    def __init__(self):
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
        #self.protocol.send_msg_mempool()
        pass

    @asyncio.coroutine
    def run(self, connection_manager, protocol):
        self.connection_manager = connection_manager
        self.protocol = protocol

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

    def stop(self):
        ## BRAIN DAMAGE: how do we get out of the loop?
        ## it seems we will hang on protocol.next_message?
        self.is_running = False

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.protocol.transport.get_extra_info("socket").getpeername())
