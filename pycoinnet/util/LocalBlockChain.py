
class LocalBlockChain(object):
    def __init__(self):
        self.parent_lookup = {}
        self.descendents_by_top = {}
        self.trees_from_bottom = {}

    def __repr__(self):
        return "<LocalBlockChain: trees_fb:%s d_b_tops:%s>" % (self.trees_from_bottom, self.descendents_by_top)

    def load_nodes(self, nodes):
        # register everything
        new_hashes = set()
        for h, parent in nodes:
            if h in self.parent_lookup:
                continue
            self.parent_lookup[h] = parent
            new_hashes.add(h)
        if new_hashes:
            self.meld_new_hashes(new_hashes)

    def meld_new_hashes(self, new_hashes):
        # make a list
        while len(new_hashes) > 0:
            h = new_hashes.pop()
            initial_bottom_h = h
            path = [h]
            while 1:
                h = self.parent_lookup.get(h)
                if h is None:
                    break
                new_hashes.discard(h)
                preceding_path = self.trees_from_bottom.get(h)
                if preceding_path:
                    del self.trees_from_bottom[h]
                    path.extend(preceding_path)
                    # we extended an existing path. Fix up descendents_by_top
                    self.descendents_by_top[preceding_path[-1]].remove(preceding_path[0])
                    break
                path.append(h)
            self.trees_from_bottom[path[0]] = path

            #if len(path) <= 1:
            #    # this is a lone element... don't bother trying to extend
            #    continue

            # now, perform extensions on any trees that start below here

            bottom_h, top_h = path[0], path[-1]

            top_descendents = self.descendents_by_top.setdefault(top_h, set())
            bottom_descendents = self.descendents_by_top.get(bottom_h)
            if bottom_descendents:
                for descendent in bottom_descendents:
                    prior_path = self.trees_from_bottom[descendent]
                    prior_path.extend(path[1:])
                    if path[0] in self.trees_from_bottom:
                        del self.trees_from_bottom[path[0]]
                    else:
                        pass#import pdb; pdb.set_trace()
                del self.descendents_by_top[bottom_h]
                top_descendents.update(bottom_descendents)
            else:
                top_descendents.add(bottom_h)

    def all_chains(self):
        for h, bottoms in self.descendents_by_top.items():
            for bottom_h in bottoms:
                yield self.trees_from_bottom[bottom_h]

    def longest_chains(self, weight_f=lambda c: len(c)):
        longest = []
        max_length = 0
        for c in self.all_chains():
            v = weight_f(c)
            if v > max_length:
                max_length = v
                longest = []
            longest.append(c)
        return longest

    def maximum_path(self, h, cache={}):
        v = self.trees_from_bottom.get(h)
        if v:
            return v
        h1 = self.parent_lookup.get(h)
        v = [h]
        if h1 is not None:
            v1 = self.maximum_path(h1, cache)
            v.extend(v1)
        cache[h] = v
        return v

    def chain_difficulty(self, chain, node_weight_f, cache={}):
        return sum(node_weight_f(c) for c in chain)

    def difficulty(self, h, node_weight_f, path_cache={}):
        return self.chain_difficulty(self.maximum_path(h, path_cache), node_weight_f)

    def longest_chains_by_difficulty(self, node_weight_f, cache={}):
        return self.longest_chains(lambda c: self.chain_difficulty(c, node_weight_f, cache))

    def find_ancestral_path(self, h1, h2, path_cache={}):
        p1 = self.maximum_path(h1, path_cache)
        p2 = self.maximum_path(h2, path_cache)
        if p1[-1] != p2[-1]:
            return [], []

        shorter_len = min(len(p1), len(p2))
        i1 = len(p1) - shorter_len
        i2 = len(p2) - shorter_len
        while 1:
            if p1[i1] == p2[i2]:
                return p1[:i1+1], p2[:i2+1]
            i1 += 1
            i2 += 1            
