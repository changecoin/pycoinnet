#!/usr/bin/env python

import binascii

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev

from pycoinnet.util import BlockChain


def header_iter(path):
    try:
        with open(path, "rb") as f:
            while 1:
                yield BlockHeader.parse(f)
    except Exception:
        pass

def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))

LOCAL_GENESIS = (bytes(reversed(binascii.unhexlify('000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214'))), 113300798888791, 250000)


def main():
    blockchain = BlockChain.BlockChain([LOCAL_GENESIS])
    iter = reversed(list(header_iter("headers.bin")))
    blockchain.load_blocks(iter)
    try:
        f = open("bc.bin", "rb")
        blockchain = BlockChain.BlockChain.parse(f)
    except Exception:
        print("failed to open and parse bc.bin")
    last_header = blockchain.longest_chain_endpoint()
    last_header = blockchain.longest_chain_endpoint()
    difficulty, block_number = blockchain.distance(last_header)
    print(b2h_rev(last_header), difficulty, block_number)
    last_header = blockchain.block_number(256789)
    difficulty, block_number = blockchain.distance(last_header)
    print(b2h_rev(last_header), difficulty, block_number)
    h1 = h2b_rev("0000000000000001a677b173b522fde9d5e60624dcb632ef3558aca65f4f65a6")
    h2 = h2b_rev("000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214")
    print(b2h_rev(blockchain.common_ancestor(h1, h2)))
    f = open("bc.bin", "wb")
    blockchain.stream(f)
    f.close()


main()
