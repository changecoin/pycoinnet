import asyncio
import os

from pycoin.encoding import double_sha256
from pycoin.serialize import h2b_rev, b2h_rev

from pycoinnet.helpers import networks
from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.standards import version_data_for_peer
from pycoinnet.helpers.standards import initial_handshake

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_MERKEL_BLOCK

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol

from pycoinnet.bloom import BloomFilter

def yield_from(f):
    return asyncio.get_event_loop().run_until_complete(f)


def protocol_factory():
    return BitcoinPeerProtocol(networks.MAINNET["MAGIC_HEADER"])


def tx_hashes_for_merkle_message(m):
    if m[0] != "merkleblock":
        raise ValueError("wrong message type")
    total_transactions = m[1]["total_transactions"]
    leaf_index_start = (1<<(total_transactions).bit_length())-1

    def recurse(level_widths, level_index, node_index, hashes, flags, flag_index, tx_acc):
        idx, r = divmod(flag_index, 8)
        mask = (1 << r)
        flag_index += 1
        if flags[idx] & mask == 0:
            h = hashes.pop()
            return h, flag_index

        if level_index == len(level_widths) - 1:
            h = hashes.pop()
            tx_acc.append(h)
            return h, flag_index

        # traverse the left
        left_hash, flag_index = recurse(
            level_widths, level_index+1, node_index*2, hashes, flags, flag_index, tx_acc)

        # is there a right?
        if node_index*2+1 < level_widths[level_index+1]:
            right_hash, flag_index = recurse(
                level_widths, level_index+1, node_index*2+1, hashes, flags, flag_index, tx_acc)

            if left_hash == right_hash:
                raise ValueError("merkle hash has same left and right value at node %d" % node_index)
        else:
            right_hash = left_hash

        return double_sha256(left_hash + right_hash), flag_index

    level_widths = []
    count = total_transactions
    while count > 1:
        level_widths.append(count)
        count += 1
        count //= 2
    level_widths.append(1)
    level_widths.reverse()

    tx_acc = []
    flags = m[1]["flags"]
    hashes = list(reversed(m[1]["hashes"]))
    left_hash, flag_index = recurse(level_widths, 0, 0, hashes, flags, 0, tx_acc)

    if len(hashes) > 0:
        raise ValueError("extra hashes: %s" % hashes)

    idx, r = divmod(flag_index-1, 8)
    if idx != len(flags) - 1:
        raise ValueError("not enough flags consumed")

    if flags[idx] > (1<<(r+1))-1:
        raise ValueError("unconsumed 1 flag bits set")

    if left_hash != m[1]["header"].merkle_root:
        raise ValueError("merkle root %s does not match calculated hash %s" % (b2h_rev(m[1]["header"].merkle_root), b2h_rev(left_hash)))

    return tx_acc

def t():
    import io
    from pycoin.block import BlockHeader
    from pycoin.serialize import h2b
    f = io.BytesIO(h2b("0100000042567d74787411cbe3b827ed483dba542e6172109529b541c75e3d000000000064668153eb40086b67142ab7717e7b8f5327b449985f7134c05fda77b57b8e854bba594c5a0c011ca70aa62a"))
    bh = BlockHeader.parse(f)
    m = ('merkleblock',
            {'header': bh,
             'total_transactions': 5,
             'hashes': [h2b_rev(x) for x in "e7bf9edd8ea7a8300298b7ace6b27d0f4631e91218164ad274aa9997803ca5b2 25006982b999b6681ebfd8ec19cb5cc292942f8a3ab8cd5d2ef0803ef5f66b6f 444a0494452c8f7f4fac7ad9687412c91aafa0fae7e60e167059c6ba18128425 859d3ce9c40374b14ea4f7faa44b9c1a044ff88703f1c2b61b01e2b358d1185c".split()],
             'flags': (251, 1)})
    txs = tx_hashes_for_merkle_message(m)
    from pycoin.serialize import b2h_rev
    for tx in txs:
        print(b2h_rev(tx))


def main():
    bf = BloomFilter(1024, hash_function_count=8, tweak=3)
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

    the_bytes, hfc, tweak = bf.filter_load_params()
    peer.send_msg("filterload", filter=the_bytes, hash_function_count=hfc, tweak=tweak, flags=0)
    h = h2b_rev("0000000000000000186831e8e00586e763deff7f6bc04436b0e988523f7fcf16")
    h = h2b_rev("0000000000bc76619da94d92e0fb82570141a827603c0f1dfb6fe06ee8c96c79")
    #h = h2b_rev("00000000001a2edf34b50bedfa2332bc6d9f47ae9bde9b038d11469006d5b114")

    items = [InvItem(ITEM_TYPE_MERKEL_BLOCK, h)]
    peer.send_msg("getdata", items=items)
    m = yield_from(next_message())
    print(m)
    txs = tx_hashes_for_merkle_message(m)
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
