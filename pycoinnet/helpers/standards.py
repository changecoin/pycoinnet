import asyncio
import os
import time

from pycoinnet.PeerAddress import PeerAddress

class BitcoinProtocolError(Exception):
    pass


def default_msg_version_parameters(peer):
    remote_ip, remote_port = peer.peername
    remote_addr = PeerAddress(1, remote_ip, remote_port)
    local_addr = PeerAddress(1, "127.0.0.1", 6111)
    d = dict(
        version=70001, subversion=b"/Notoshi/", services=1, timestamp=int(time.time()),
        remote_address=remote_addr, local_address=local_addr,
        nonce=int.from_bytes(os.urandom(8), byteorder="big"),
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
    @asyncio.coroutine
    def ping_task(next_message):
        while True:
            try:
                r = yield from asyncio.wait_for(next_message(), timeout=heartbeat_rate)
                continue
            except asyncio.TimeoutError as ex:
                pass
            # oh oh! no messages
            # send a ping
            nonce = int.from_bytes(os.urandom(8), byteorder="big")
            peer.send_msg("ping", nonce=nonce)
            end_time = time.time() + missing_pong_disconnect_timeout
            while True:
                try:
                    timeout = end_time - time.time()
                    name, data = yield from asyncio.wait_for(next_message(), timeout=timeout)
                    if name == "pong" and data["nonce"] == nonce:
                        break
                except asyncio.TimeoutError:
                    peer.connection_lost(None)
                    logging.error("remote peer %s didn't answer ping, disconnecting", peer)
                    return
    next_message = peer.new_get_next_message_f()
    asyncio.Task(ping_task(next_message))

def install_pong_manager(peer):
    @asyncio.coroutine
    def pong_task(next_message):
        while True:
            name, data = yield from next_message()
            assert name == 'ping'
            peer.send_msg("pong", nonce=data["nonce"])
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'ping')
    asyncio.Task(pong_task(next_message))

@asyncio.coroutine
def get_date_address_tuples(peer):
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'addr')
    peer.send_msg("getaddr")
    name, data = yield from next_message()
    return data["date_address_tuples"]

@asyncio.coroutine
def get_headers_hashes(peer, after_block_hash):
    hashes = [after_block_hash]
    peer.send_msg(message_name="getheaders", version=1, hashes=hashes, hash_stop=after_block_hash)
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'headers')
    name, data = yield from next_message()
    headers = [bh for bh, t in data["headers"]]
    return headers
