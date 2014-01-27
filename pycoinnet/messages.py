import io

from pycoin.serialize import bitcoin_streamer
from pycoinnet.reader import init_bitcoin_streamer

init_bitcoin_streamer()


class BitcoinProtocolMessage(object):
    """
    name: message name
    data: unparsed blob
    """
    def __init__(self, message_name, data):
        self.name = message_name
        message_stream = io.BytesIO(data)
        parser = MESSAGE_PARSERS.get(message_name)
        if parser:
            d = parser(message_stream)
            fixup = MESSAGE_FIXUPS.get(message_name)
            if fixup:
                d = fixup(d, message_stream)
        else:
            logging.error("unknown message: %s %s", message_name, binascii.hexlify(data))
            d = {}
        for k, v in d.items():
            setattr(self, k, v)

    def __str__(self):
        return "<BitcoinProtocolMessage %s>" % self.name


MESSAGE_STRUCTURES = {
    'version': "version:L services:Q timestamp:Q remote_address:A local_address:A"
                " nonce:Q subversion:S last_block_index:L",
    'verack': "",
    'inv': "items:[v]",
    'getdata': "items:[v]",
    'addr': "date_address_tuples:[LA]",
    'alert': "payload signature:SS",
    'tx': "tx:T",
    'block': "block:B",
    'ping': "nonce:Q",
    'pong': "nonce:Q",
}


def _make_parser(the_struct=''):
    def f(message_stream):
        struct_items = [s.split(":") for s in the_struct.split()]
        names = [s[0] for s in struct_items]
        types = ''.join(s[1] for s in struct_items)
        return bitcoin_streamer.parse_as_dict(names, types, message_stream)
    return f


def message_parsers():
    return dict((k, _make_parser(v)) for k, v in MESSAGE_STRUCTURES.items())


def message_fixups():
    def fixup_version(d, f):
        if d["version"] >= 70001:
            b = f.read(1)
            if len(b) > 0:
                d["relay"] = (ord(b) != 0)
        return d

    alert_submessage_parser = _make_parser(
        "version:L relayUntil:Q expiration:Q id:L cancel:L setCancel:S minVer:L "
        "maxVer:L setSubVer:S priority:L comment:S statusBar:S reserved:S")

    def fixup_alert(d, f):
        d1 = alert_submessage_parser(f)
        d["alert_info"] = d1
        return d

    return {
        'version': fixup_version,
        'alert': fixup_alert
    }


MESSAGE_PARSERS = message_parsers()
MESSAGE_FIXUPS = message_fixups()
