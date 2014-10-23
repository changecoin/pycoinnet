
from pycoin.serialize import b2h_rev

"""blockchain_view:
- tuples of (index, hash, total_work)

- needed here

.last_block_index()
.block_locator_hashes()
.got_headers(headers)
"""

HASH_INITIAL_BLOCK = b'\0' * 32


class BlockChainView:
    def __init__(self, node_tuples=[]):
        """
        A node_tuple is (index, hash, total_work).
        """
        self.node_tuples = []
        self.hash_to_index = dict()
        self._add_tuples(node_tuples)

    def _add_tuples(self, node_tuples):
        nt = set(node_tuples)
        self.node_tuples = sorted(set(self.node_tuples).union(nt))
        self.hash_to_index.update(dict((h, idx) for idx, h, tw in nt))

    def last_block_index(self):
        if len(self.node_tuples) == 0:
            return 0
        return self.node_tuples[-1][0]

    def tuple_for_index(self, index):
        """
        Return the node with the largest index <= the given index.
        In other words, this is the node we need to rewind to.
        """
        lo = 0
        hi = len(self.node_tuples)
        while lo < hi:
            idx = int((lo+hi)/2)
            if self.node_tuples[idx][0] > index:
                hi = idx
            else:
                lo = idx + 1
        return self.node_tuples[hi-1]

    def tuple_for_hash(self, hash):
        idx = self.hash_to_index.get(hash)
        if idx is not None:
            return self.tuple_for_index(idx)
        return None

    def key_index_generator(self):
        index = self.last_block_index() - 1
        step_size = 1
        count = 10
        while index > 0:
            yield index
            index -= step_size
            count -= 1
            if count <= 0:
                count = 10
                step_size *= 2
        yield 0

    def block_locator_hashes(self):
        """
        Generate locator_hashes value suitable for passing to getheaders message.
        """
        l = []
        for index in self.key_index_generator():
            the_hash = self.tuple_for_index(index)[1]
            if len(l) == 0 or the_hash != l[-1]:
                l.append(the_hash)
        l.reverse()
        if len(l) == 0:
            l.append(b'\0' * 32)
        return l

    def do_headers_improve_path(self, headers):
        """
        Raises ValueError if headers path don't extend from anywhere in this view.
        """
        if len(self.node_tuples) == 0:
            if headers[0].previous_block_hash != HASH_INITIAL_BLOCK:
                return False
            the_tuple = (-1, HASH_INITIAL_BLOCK, -1)
        else:
            the_tuple = self.tuple_for_hash(headers[0].previous_block_hash)
            if the_tuple is None:
                return False
        total_work = the_tuple[0]  ## TODO: make this difficulty/work instead of path size
        expected_prior_hash = the_tuple[1]
        for h in headers:
            if h.previous_block_hash != expected_prior_hash:
                raise ValueError(
                    "headers are not properly linked: no known block with hash %s" % b2h_rev(h.previous_block_hash))
            total_work += 1  ## TODO: make this difficult/work instead of path size
            expected_prior_hash = h.hash()
        if total_work < self.last_block_index():
            return False

        old_end_idx = self.last_block_index() + 1
        base_idx = the_tuple[0] + 1
        base_work = the_tuple[-1]
        self._add_tuples((idx+base_idx, h.hash(), base_work+h.difficulty) for idx, h in enumerate(headers))
        new_start_idx = base_idx + len(headers)
        # report the deltas
        # (old_end_idx, new_start_idx, headers)
        # usually old_end_idx and new_start_idx are the same
        # if they're not, we go new_start_idx to old_end_idx+1 by -1 and remove those block indices
        report = (old_end_idx, new_start_idx, headers)
        return report

    @staticmethod
    def _halsies_indices(block_index):
        s = set()
        step_size = 1
        count = 2
        while block_index >= 0:
            s.add(block_index)
            block_index -= step_size
            count -= 1
            if count <= 0 and block_index % (step_size*2) == 0:
                step_size *= 2
                count = 2
        return s

    def winnow(self):
        """
        This method thins out the node_tuples using the "halfsies" method.
        """
        halfsies_indices = self._halsies_indices(self.last_block_index())
        old_node_tuples = self.node_tuples
        self.node_tuples = []
        self.hash_to_index = dict()
        self._add_tuples(t for t in old_node_tuples if t[0] in halfsies_indices)
