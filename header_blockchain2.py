#!/usr/bin/env python

import binascii

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev

from pycoinnet.util import BlockChain2


def header_iter(path):
    try:
        with open(path, "rb") as f:
            while 1:
                yield BlockHeader.parse(f)
    except Exception:
        pass

def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))

LOCAL_GENESIS_HASH = bytes(reversed(binascii.unhexlify('000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214')))

LOCAL_GENESIS = BlockChain2.genesis_block_to_block_chain_record(LOCAL_GENESIS_HASH, difficulty=0, index_difficulty=(250000, 113300798888791))


def main():
    blockchain = BlockChain2.BlockChain()
    blockchain.load_records([LOCAL_GENESIS])
    iter = reversed(list(header_iter("headers.bin")))
    blockchain.load_records(BlockChain2.block_header_to_block_chain_record(bh) for bh in iter)
    try:
        raise Exception("foo")
        #import pdb; pdb.set_trace()
        f = open("bc.bin", "rb")
        blockchain = BlockChain2.BlockChain.parse(f)
    except Exception:
        print("failed to open and parse bc.bin")
    last_header = blockchain.longest_chain_endpoint()
    difficulty, block_number = blockchain.distance(last_header)
    print(b2h_rev(last_header), difficulty, block_number)
    last_header = blockchain.block_by_number(256789)
    difficulty, block_number = blockchain.distance(last_header)
    print(b2h_rev(last_header), difficulty, block_number)
    last_header = blockchain.block_by_number(250000)
    difficulty, block_number = blockchain.distance(last_header)
    print(b2h_rev(last_header), difficulty, block_number)
    h1 = h2b_rev("0000000000000001a677b173b522fde9d5e60624dcb632ef3558aca65f4f65a6")
    h2 = h2b_rev("000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214")
    print(b2h_rev(blockchain.common_ancestor(h1, h2)))
    f = open("bc.bin", "wb")
    blockchain.stream(f)
    f.close()


main()
