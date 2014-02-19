import asyncio
import os
import struct
import time


class BitcoinProtocolError(Exception):
    pass


def default_msg_version_parameters(peer):
    import os, random, struct
    remote_ip, remote_port = peer.peername
    remote_addr = PeerAddress(1, remote_ip, remote_port)
    local_addr = PeerAddress(1, "127.0.0.1", 6111)
    d = dict(
        version=70001, subversion=b"/Notoshi/", services=1, timestamp=int(time.time()),
        remote_address=remote_addr, local_address=local_addr,
        nonce=struct.unpack("!Q", os.urandom(8))[0],
        last_block_index=0, want_relay=True
    )
    return d


@asyncio.coroutine
def initial_handshake(peer, version_parameters):
    # do handshake

    next_message = peer.new_get_next_message_f()
    peer.send_msg("version", **version_parameters)

    message_name, version_data = yield from next_message()
    if message_name != 'version':
        raise BitcoinProtocolError("missing version")
    peer.send_msg("verack")

    message_name, data = yield from next_message()
    if message_name != 'verack':
        raise BitcoinProtocolError("missing verack")

    return version_data

def install_ping_manager(peer, heartbeat_rate=60, missing_pong_disconnect_timeout=60):
    def ping_task():
        next_message = peer.new_get_next_message_f()
        while True:
            try:
                r = asyncio.wait_for(next_message(), timeout=heartbeat_rate)
                continue
            except TimeoutError:
                pass
            # oh oh! no messages
            # send a ping
            nonce = struct.unpack("!Q", os.urandom(8))[0]
            peer.send_msg("ping", nonce=nonce)
            timeout = time.time() + missing_pong_disconnect_timeout
            while True:
                try:
                    name, data = asyncio.wait_for(next_message(), timeout=time.time() - timeout)
                    if name == "pong" and data["nonce"] == nonce:
                        break
                except TimeoutError:
                    peer.connection_lost(None)
                    return
    asyncio.Task(ping_task())

@asyncio.coroutine
def install_pong_manager(peer):
    def pong_task():
        next_message = peer.new_get_next_message_f(lambda name, data: name == 'ping')
        while True:
            name, data = next_message()
            assert name == 'pong'
            peer.send_msg("pong", nonce=data["nonce"])
    asyncio.Task(pong_task())




def create_inv_item_for_peer_f(peer):
    def fetch_inv_item(inv_item):
        pass
    pass


class InvItemHandler:
    def __init__(self, peer):
        self.inv_items_requested = Queue()
        self.inv_item_futures = weakref.WeakValueDictionary()

        peer.register_delegate(self)
        self.peer = peer
        peer.request_inv_item = self.request_inv_item
        peer.request_inv_item_future = self.request_inv_item_future
        asyncio.Task(self.getdata_sender())

        next_message = peer.new_get_next_message_f(filter_f=lambda name, data: name in ["tx", "block", "notfound"])
        asyncio.Task(self.fetchitems_loop(next_message))

    def fetchitems_loop(self, next_message):
        while True:
            name, data = yield from next_message()
            if name == "tx":
                self.fulfill(ITEM_TYPE_TX, data["tx"])
            if name == "block":
                self.fulfill(ITEM_TYPE_BLOCK, data["block"])
            if name == "notfound":
                for inv_item in data["items"]:
                    future = self.inv_item_futures.get(inv_item)
                    if future:
                        del self.inv_item_futures[inv_item]
                        future.cancel()

    def backlog(self):
        return len(self.inv_item_futures) + self.inv_items_requested.size()

    def fulfill(self, inv_item_type, item):
        inv_item = InvItem(inv_item_type, item.hash())
        future = self.inv_item_futures.get(inv_item)
        if future:
            del self.inv_item_futures[inv_item]
            if not future.done():
                future.set_result(item)
            else:
                logging.info("got %s unsolicited", item.id())

    def request_inv_item(self, inv_item):
        future = asyncio.Future()
        self.request_inv_item_future(inv_item, future)
        yield from asyncio.wait_for(future, timeout=None)
        return future.result()

    def request_inv_item_future(self, inv_item, future):
        logging.debug("request inv item %s", inv_item)
        self.inv_items_requested.put_nowait((inv_item, future))

    @asyncio.coroutine
    def getdata_sender(self):
        while True:
            pairs = yield from self.inv_items_requested.get_all()
            so_far = []
            for inv_item, future in pairs:
                if future.cancelled():
                    continue
                so_far.append(inv_item)
                self.inv_item_futures[inv_item] = future
                if len(so_far) >= 50000:
                    self.peer.send_msg("getdata", items=so_far)
                    so_far = []
            if len(so_far) > 0:
                self.peer.send_msg("getdata", items=so_far)
