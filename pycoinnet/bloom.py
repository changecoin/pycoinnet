
class BloomFilter(object):
    def __init__(self, size_in_bytes, hash_function_count, tweak):
        self.filter_bytes = bytearray(size_in_bytes)
        self.bit_count = 8 * size_in_bytes
        self.hash_function_count = hash_function_count
        self.tweak = tweak

    def add_item(self, item_bytes):
        for hash_index in range(self.hash_function_count):
            seed = hash_index * 0xFBA4C795 + self.tweak
            self.set_bit(murmur3(item_bytes), seed=seed)

    def _index_for_bit(self, v):
        v %= self.bit_count
        byte_index, mask_index = divmod(v, 8)
        mask = [1, 2, 4, 8, 16, 32, 64, 128][mask_index]
        return byte_index, mask

    def set_bit(self, v):
        byte_index, mask = self._index_for_bit(v)
        self.filter_bytes[byte_index] |= mask

    def check_bit(self, v):
        byte_index, mask = self._index_for_bit(self, v)
        return (self.filter_bytes[byte_index] & mask) == mask

    def filter_load_params(self):
        return self.filter_bytes, self.hash_function_count, self.tweak


# http://stackoverflow.com/questions/13305290/is-there-a-pure-python-implementation-of-murmurhash

def murmur3(data, seed=0):
    c1 = 0xcc9e2d51
    c2 = 0x1b873593

    length = len(data)
    h1 = seed
    roundedEnd = (length & 0xfffffffc)  # round down to 4 byte block
    for i in range(0, roundedEnd, 4):
      # little endian load order
      k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | \
           ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24)
      k1 *= c1
      k1 = (k1 << 15) | ((k1 & 0xffffffff) >> 17) # ROTL32(k1,15)
      k1 *= c2

      h1 ^= k1
      h1 = (h1 << 13) | ((h1 & 0xffffffff) >> 19)  # ROTL32(h1,13)
      h1 = h1 * 5 + 0xe6546b64

    # tail
    k1 = 0

    val = length & 0x03
    if val == 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16
    # fallthrough
    if val in [2, 3]:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8
    # fallthrough
    if val in [1, 2, 3]:
        k1 |= data[roundedEnd] & 0xff
        k1 *= c1
        k1 = (k1 << 15) | ((k1 & 0xffffffff) >> 17)  # ROTL32(k1,15)
        k1 *= c2
        h1 ^= k1

    # finalization
    h1 ^= length

    # fmix(h1)
    h1 ^= ((h1 & 0xffffffff) >> 16)
    h1 *= 0x85ebca6b
    h1 ^= ((h1 & 0xffffffff) >> 13)
    h1 *= 0xc2b2ae35
    h1 ^= ((h1 & 0xffffffff) >> 16)

    return h1 & 0xffffffff
