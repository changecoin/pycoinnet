"""
Directory structure looks like this:

+ block-archive
  + block-280XXX
    - block-280000-0000000000000001c091ada69f444dc0282ecaabe4808ddbb2532e5555db0c03.bin
    - block-280001-0000000000000000bc7b8f8b4a60aeb73c05de005797af3b78e84d61c93f3d15.bin
  + orphans
    - block-280000-00000000000000000862a3f60a39f3e18a6e670765bb5b582ca325fcf7ab5697.bin
+ block-updates
  + update-1400000000
    - block-280002-0000000000000002439e3f01a020d3539cd336c922071f55b8f07ef8eddb128b.bin
    - block-280003-00000000000000011482b8022e8984df6ec1ed1213201ec37d99e730d022684f.bin
    - block-280004-000000000000000158cbcc7acdc9fbb162fdeb2f1c09530dd14aa876f0dc9594.bin
  + update-1400001000
    - block-280005-00000000000000012707853ce0885f39198c2ab29886a21b7f4544415e94f71d.bin
+ tx-archives
  + block-280XXX
    - txs-in-block-280004-000000000000000158cbcc7acdc9fbb162fdeb2f1c09530dd14aa876f0dc9594.txt
    - txs-in-block-280005-00000000000000012707853ce0885f39198c2ab29886a21b7f4544415e94f71d.txt
  + orphans
    - txs-in-block-280000-00000000000000000862a3f60a39f3e18a6e670765bb5b582ca325fcf7ab5697.txt
+ trash
  - a place to put stuff that can be deleted. Move them quickly, atomically

What will we do with this file?

- push a reorg (new blocks, possibly rewind)
- iterate over new blocks looking for relevant transactions to archive
- re-iterate over archived blocks looking for relevant transactions to archive
- iterate over transactions starting at block N
- re-iterate over archived transactions starting at block N

- perform fsck of ExternalTx or Spendable vs. archived transactions
  - build an index of all transactions and by tx id and Spendable id
  (re-iterate over archived with N = 0)


Use case:
- create_block_update(blocks, block_base_index)

- filter_block_updates(rewind_to_block_f, should_keep_tx_f)
- tx_updates(tx_f)

- refilter_block_updates(block_base_index, should_keep_tx_f)
- create_tx_update_from_archive(block_base_index)

call out to:

- reorg_to_block_index(block_index)

"""

import logging
import re
import os
import tempfile
import time

from pycoin.block import Block, BlockHeader
from pycoin.tx import Tx

BLOCK_UPDATES_PATH = "updates"
BLOCK_ARCHIVE_PATH = "block-archive"

TX_ARCHIVE_PATH = "tx-archive"

ORPHANS_PATH = "orphans"
TRASH_PATH = "trash"


UPDATE_PATH_CRE = re.compile(r"^update-([0-9]{10})$")
BLOCK_NAME_CRE = re.compile("block-(?P<block_index>[0-9]{6})-(?P<block_hash>[0-9a-f]{64}).bin$")
MIDLEVEL_NAME_CRE = re.compile("^block-(?P<block_index_thousands>[0-9]{3})XXX$")
TX_METADATA_CRE = re.compile("txs-in-block-(?P<block_index>[0-9]{6})-(?P<block_hash>[0-9a-f]{64}).txt")


def new_block_parse(f):
    """Parse the Block from the file-like object in the standard way
    that blocks are sent in the network."""
    # TODO: move to pycoin
    from pycoin.tx import Tx
    from pycoin.serialize.bitcoin_streamer import parse_struct
    (version, previous_block_hash, merkle_root, timestamp,
        difficulty, nonce, count) = parse_struct("L##LLLI", f)
    txs = []
    for i in range(count):
        offset_in_block = f.tell()
        tx = Tx.parse(f)
        txs.append(tx)
        tx.offset_in_block = offset_in_block
    block = Block(version, previous_block_hash, merkle_root, timestamp, difficulty, nonce, txs)
    for tx in txs:
        tx.block = block
    return block


def makedir(p):
    if not os.path.exists(p):
        os.makedirs(p)


def block_name(block_id, block_index):
    return "block-%06d-%s.bin" % (block_index, block_id)


def metadata_name(block_index, block_id):
    return "txs-in-block-%06d-%s.txt" % (block_index, block_id)


class BlockArchive(object):
    def __init__(self, base_path):
        self.base_path = base_path

    def block_updates_base_path(self):
        return os.path.join(self.base_path, BLOCK_UPDATES_PATH)

    def create_block_update(self, blocks, base_block_index, timestamp=None):
        """
        Create a new update: a directory named update-XXXXXXXXXX (X is a digit 0-9)
        with the list of new blocks.
        """
        while 1:
            if timestamp is None:
                timestamp = int(time.time())
            path = os.path.join(self.block_updates_base_path(), "update-%10d" % timestamp)
            if os.path.exists(path):
                timestamp = None
                time.sleep(1)
            else:
                break
        tmp_path = "%s.tmp" % path
        makedir(tmp_path)
        for idx, b in enumerate(blocks):
            f = open(os.path.join(tmp_path, block_name(b.id(), base_block_index + idx)), "wb")
            b.stream(f)
            f.close()
        os.rename(tmp_path, path)

    def block_updates(self):
        """
        Return a iterator corresponding to pending update-XXXXXXXXXX directories.
        It yields an iterator2, source_path, where iterator2 is an
        iterator returning: block, block_path, block_index, block_id
        """
        def iterator2(path):
            paths = os.listdir(path)
            for p in paths:
                m = BLOCK_NAME_CRE.match(p)
                if m is None:
                    continue
                try:
                    block_path = os.path.join(path, p)
                    block = new_block_parse(open(block_path, "rb"))
                    block_index = int(m.group("block_index"))
                    block_id = m.group("block_hash")
                    yield block, block_path, block_index, block_id
                except Exception:
                    pass

        base = self.block_updates_base_path()
        try:
            l = os.listdir(base)
            l.sort()
            for p in l:
                m = UPDATE_PATH_CRE.match(p)
                if m is None:
                    continue
                base_name = m.group(0)
                path = os.path.join(base, base_name)
                yield iterator2(path), base_name
        except Exception:
            pass

    def is_reorg_pending(self):
        for j in self.block_updates():
            # if there are any results, return True. Cute, eh?
            return True
        return False

    def archive_block(self, block_path):
        """
        Move the block to the block-archive directory.
        """
        m = BLOCK_NAME_CRE.search(block_path)
        block_index = int(m.group("block_index"))
        midlevel_path = "block-%03dXXX" % (block_index // 1000)
        base = os.path.join(self.base_path, BLOCK_ARCHIVE_PATH, midlevel_path)
        makedir(base)
        new_path = os.path.join(base, m.group(0))
        if not os.path.exists(new_path):
            os.link(block_path, new_path)

    def block_info_exceeding(self, min_block_index):
        """
        An iterator of block info for blocks with index that exceed min_block_index.
        """
        base = os.path.join(self.base_path, BLOCK_ARCHIVE_PATH)
        midlevel_path_matches = [MIDLEVEL_NAME_CRE.match(p) for p in sorted(os.listdir(base))]
        block_index_thousands = min_block_index // 1000
        for m in midlevel_path_matches:
            if m is None:
                continue
            if int(m.group("block_index_thousands")) < block_index_thousands:
                continue
            path = m.group(0)
            l = [BLOCK_NAME_CRE.match(p) for p in os.listdir(os.path.join(base, path))]
            for m in l:
                if m is None:
                    continue
                block_index = int(m.group("block_index"))
                if block_index >= min_block_index:
                    yield os.path.join(base, path, m.group(0)), block_index, m.group("block_hash")

    def block_paths_exceeding(self, min_block_index):
        """
        An iterator of paths to archived block files with index that exceed min_block_index.
        """
        for path, bi, bh in self.block_info_exceeding(min_block_index):
            yield path

    def block_path_for_index(self, block_index):
        midlevel_path = "block-%03dXXX" % (block_index // 1000)
        path = os.path.join(self.base_path, midlevel_path)
        for p in os.listdir(path):
            m = BLOCK_NAME_CRE.match(p)
            if m is None:
                continue
            the_block_index = int(m.group("block_index"))
            if the_block_index != block_index:
                continue
            return path
        return None

    def refilter_blocks(self, min_block_index, should_keep_tx_f):
        for block_path, block_index, block_id in self.block_info_exceeding(min_block_index):
            try:
                saved_txs = []
                block = new_block_parse(open(block_path, "rb"))
                logging.info("refiltering block %d %s", block_index, block_id)
                for tx_index, tx in enumerate(block.txs):
                    if should_keep_tx_f(tx):
                        logging.info("found tx %s to keep", tx.id())
                        saved_txs.append(tx)
                if len(saved_txs) > 0:
                    self.write_tx_metadata(block_index, block, saved_txs)
            except Exception:
                logging.exception("problem with %s", block_path)

    def write_tx_metadata(self, block_index, block, saved_txs):
        midlevel_path = "block-%03dXXX" % (block_index // 1000)
        dir_name = os.path.join(self.base_path, TX_ARCHIVE_PATH, midlevel_path)
        makedir(dir_name)
        path = os.path.join(dir_name, metadata_name(block_index, block.id()))
        tmp_path = "%s.tmp" % path
        f = open(tmp_path, "w")
        for tx in saved_txs:
            f.write("%s %d\n" % (tx.id(), tx.offset_in_block))
        f.close()
        os.rename(tmp_path, path)

    def orphan_block_archive(self, block_index):
        """
        Move archived blocks with index >= block_index to the orphans directory.
        """
        base = os.path.join(self.base_path, BLOCK_ARCHIVE_PATH, ORPHANS_PATH)
        makedir(base)
        for path in self.block_paths_exceeding(block_index):
            new_path = os.path.basename(path)
            os.rename(path, os.path.join(base, new_path))

    def orphan_tx_archive(self, block_index):
        """
        Move archived transaction indices with index >= block_index to the orphans directory.
        """
        base = os.path.join(self.base_path, TX_ARCHIVE_PATH, ORPHANS_PATH)
        makedir(base)
        for path in self.tx_paths_exceeding(block_index):
            new_path = os.path.basename(path)
            os.rename(path, os.path.join(base, new_path))

    def filter_block_updates(self, rewind_to_block_f, should_keep_tx_f, process_tx_f):
        """
        This function looks in the block-updates directory, iterates over each transaction in each
        block, calling should_keep_tx_f(tx, block_index) for each transaction. If this function
        returns True, a reference to the transaction is indexed in the tx-archive directory,
        and process_tx_f(block, block_index, tx) is called.

        The blocks are then copied to the archive area, then, the block-update/update-XXXXX
        directory is moved to the trash.
        """
        is_first_block_index = True
        for iterator, update_path in self.block_updates():
            for block, block_path, block_index, block_id in iterator:
                if is_first_block_index:
                    logging.info("rewinding back to block %d" % block_index)
                    rewind_to_block_f(block_index)
                    is_first_block_index = False
                logging.info("looking for txs in block %d id %s" % (block_index, block_id))
                saved_txs = []
                for tx_index, tx in enumerate(block.txs):
                    if should_keep_tx_f(tx):
                        logging.info("found tx %s to keep", tx.id())
                        saved_txs.append(tx)
                if len(saved_txs) > 0:
                    self.write_tx_metadata(block_index, block, saved_txs)
                for tx in saved_txs:
                    process_tx_f(block, block_index, tx)
                self.archive_block(block_path)
            self.move_to_trash(os.path.join(self.block_updates_base_path(), update_path))

    def block_db(self, min_block_index):
        db = {}
        for path, bi, bh in self.block_info_exceeding(min_block_index):
            db[bi] = dict(path=path, id=bh)
        return db

    def tx_info_exceeding(self, min_block_index):
        """
        An iterator of paths to archived tx files with block index that exceed min_block_index.
        """
        base = os.path.join(self.base_path, TX_ARCHIVE_PATH)
        midlevel_path_matches = [MIDLEVEL_NAME_CRE.match(p) for p in sorted(os.listdir(base))]
        block_index_thousands = min_block_index // 1000
        for m in midlevel_path_matches:
            if m is None:
                continue
            if int(m.group("block_index_thousands")) < block_index_thousands:
                continue
            path = m.group(0)
            l = [TX_METADATA_CRE.match(p) for p in os.listdir(os.path.join(base, path))]
            for m in l:
                if m is None:
                    continue
                block_index = int(m.group("block_index"))
                if block_index >= min_block_index:
                    yield os.path.join(base, path, m.group(0)), block_index, m.group("block_hash")

    def tx_paths_exceeding(self, min_block_index):
        for path, bi, bh in self.tx_info_exceeding(min_block_index):
            yield path

    def txs(self, min_block_index=-1):
        block_info_iter = self.block_info_exceeding(min_block_index)
        try:
            block_index = -1
            for tx_path, tx_block_index, tx_block_id in self.tx_info_exceeding(min_block_index):
                if block_index < tx_block_index:
                    while block_index < tx_block_index:
                        block_path, block_index, block_id = next(block_info_iter)
                    block_f = open(block_path, "rb")
                f = open(tx_path)
                for l in f:
                    tx_id, offset = l.split()
                    block_f.seek(int(offset))
                    tx = Tx.parse(block_f)
                    if tx.id() == tx_id:
                        yield tx, tx_id, block_index, block_id
        except Exception:
            logging.exception("exception in txs")

    def reprocess_txs(self, block_index_to_rewind_to, process_tx_f):
        block_db = self.block_db(block_index_to_rewind_to)
        for tx_path, block_index, block_hash in self.tx_info_exceeding(block_index_to_rewind_to):
            block_info = block_db[block_index]
            block_path = block_info["path"]
            if block_hash != block_info["id"]:
                raise IOError("bad path %s for hash %s" % (block_path, block_hash))
            block_f = open(block_path, "rb")
            block_header = BlockHeader.parse(block_f)
            f = open(tx_path)
            for l in f:
                the_hash, the_offset = l.split()
                block_f.seek(int(the_offset))
                tx = Tx.parse(block_f)
                if tx.id() != the_hash:
                    raise ValueError("tx %s not in correct place according to %s" % (the_hash, tx_path))
                process_tx_f(block_header, block_index, tx)
            f.close()
            block_f.close()

    ## functions to deal with the trash

    def move_to_trash(self, src_path):
        base_trash = os.path.join(self.base_path, TRASH_PATH)
        makedir(base_trash)
        tempdir = tempfile.mkdtemp(dir=base_trash)
        os.rename(src_path, tempdir)

    def empty_trash(self):
        base_trash = os.path.join(self.base_path, TRASH_PATH)
        dirpaths = []
        for dirpath, dirnames, filenames in os.walk(base_trash):
            for fn in filenames:
                p = os.path.join(dirpath, fn)
                logging.info("removing %s" % p)
                try:
                    os.unlink(p)
                except OSError:
                    pass
            for dn in dirnames:
                p = os.path.join(dirpath, dn)
                dirpaths.append(p)
        for p in dirpaths:
            logging.info("removing %s" % p)
            try:
                os.rmdir(p)
            except OSError:
                pass
