import asyncio
import os

from pycoin.serialize import b2h_rev, h2b, h2b_rev

from pycoinnet.helpers import networks
from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.standards import version_data_for_peer
from pycoinnet.helpers.standards import initial_handshake

from pycoinnet.InvItem import InvItem, ITEM_TYPE_MERKLEBLOCK

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol

from pycoinnet.bloom import BloomFilter


def yield_from(f):
    return asyncio.get_event_loop().run_until_complete(f)


def protocol_factory():
    return BitcoinPeerProtocol(networks.MAINNET["MAGIC_HEADER"])


def t():
    import io
    from pycoin.block import BlockHeader
    from pycoin.serialize import h2b
    f = io.BytesIO(h2b("0100000042567d74787411cbe3b827ed483dba542e6172109529b541c75e3d000000000064668153eb"
                       "40086b67142ab7717e7b8f5327b449985f7134c05fda77b57b8e854bba594c5a0c011ca70aa62a"))
    bh = BlockHeader.parse(f)
    m = ('merkleblock', {
         'header': bh,
         'total_transactions': 5,
         'hashes': [h2b_rev(x) for x in [
             "e7bf9edd8ea7a8300298b7ace6b27d0f4631e91218164ad274aa9997803ca5b2",
             "25006982b999b6681ebfd8ec19cb5cc292942f8a3ab8cd5d2ef0803ef5f66b6f",
             "444a0494452c8f7f4fac7ad9687412c91aafa0fae7e60e167059c6ba18128425",
             "859d3ce9c40374b14ea4f7faa44b9c1a044ff88703f1c2b61b01e2b358d1185c"]],
         'flags': (251, 1)})
    txs = m["tx_hashes"]
    for tx in txs:
        print(b2h_rev(tx))


def main():
    bf = BloomFilter(1024, hash_function_count=8, tweak=3)

    if 0:
        for i in range(0, 1024*8):
            if i % 4 != 3:
                bf.set_bit(i)

    network = networks.MAINNET
    host_port_q = dns_bootstrap_host_port_q(network)
    host, port = yield_from(host_port_q.get())
    host_port_q = None
    host = "71.84.28.2"
    print(host, port)

    task = asyncio.get_event_loop().create_connection(protocol_factory, host=host, port=port)
    transport, peer = yield_from(task)
    print("1: %s" % str(peer))
    r = yield_from(peer.connection_made_future)
    print(r)

    version_parameters = version_data_for_peer(
        peer, local_port=0, last_block_index=0, nonce=int.from_bytes(os.urandom(8), byteorder="big"),
        subversion="/Notoshi/".encode("utf8"))
    version_data = yield_from(initial_handshake(peer, version_parameters))
    print(version_data)

    next_message = peer.new_get_next_message_f()
    peer.send_msg("mempool")
    m = yield_from(next_message())

    # Test the hash of the transaction itself.
    if 0:
        tx_hash = h2b_rev("3e5bff948585e292fa0b043b62de4019787ffdf872f6cb21e124c204d09be88a")
        bf.add_item(tx_hash)

    # For each output, test each data element of the output script. This means each hash
    # and key in the output script is tested independently. Important: if an output matches
    # whilst testing a transaction, the node might need to update the filter by inserting
    # the serialized COutPoint structure. See below for more details.
    if 1:
        from pycoin.encoding import bitcoin_address_to_hash160_sec
        address = "1bonesyuMYEZQBQmy4Z6N9gpoNHqx8Dev"
        the_hash160 = bitcoin_address_to_hash160_sec(address)
        bf.add_item(the_hash160)

    # For each input, test the serialized COutPoint structure.
    if 0:
        # Spendable
        the_c_out = (h2b_rev("3e5bff948585e292fa0b043b62de4019787ffdf872f6cb21e124c204d09be88a")
                     + b'\0\0\0\0')
        bf.add_item(the_c_out)

    # For each input, test each data element of the input script
    # (note: input scripts only ever contain data elements).
    if 0:
        data_element = h2b("304402202b30dba17d6cc3926f6fb317509b68cb877f1287bc12cfc1168811071508bd74"
                           "022065af95136076450ba6064b48b6ac583dd40a1c7d5d454c2a7faf4014a111112501")
        bf.add_item(data_element)

    # Otherwise there is no match.

    the_bytes, hfc, tweak = bf.filter_load_params()
    peer.send_msg("filterload", filter=the_bytes, hash_function_count=hfc, tweak=tweak, flags=0)
    h = h2b_rev("0000000000000000186831e8e00586e763deff7f6bc04436b0e988523f7fcf16")
    h = h2b_rev("0000000000bc76619da94d92e0fb82570141a827603c0f1dfb6fe06ee8c96c79")
    h = h2b_rev("00000000000000000fe003fb653d25916a0c58bf952c5412664e5fa7063761fd")

    items = [InvItem(ITEM_TYPE_MERKLEBLOCK, h)]
    peer.send_msg("getdata", items=items)
    while 1:
        m = yield_from(next_message())
        print(m)
        if m[0] == "merkleblock":
            break
    txs = m["tx_hashes"]
    print(txs)
    while 1:
        m = yield_from(next_message())
        print(m)
    import io
    f = io.BytesIO()
    m[1]["header"].stream(f)
    from pycoin.serialize import b2h
    print(b2h(f.getvalue()))

main()
