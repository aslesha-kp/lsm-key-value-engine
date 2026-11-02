import struct
import zlib

import pytest

from lsmkv.errors import CorruptionError
from lsmkv.sstable import HEADER, FOOTER, ENTRY, encode, decode


def raw(entries, *, count=None, minimum=None, maximum=None):
    body = HEADER + b''.join(struct.pack('<IIQB', len(k), len(v), seq, op) + k + v
                             for k, seq, op, v in entries)
    sequences = [entry[1] for entry in entries]
    return body + FOOTER.pack(len(entries) if count is None else count,
                             min(sequences) if minimum is None else minimum,
                             max(sequences) if maximum is None else maximum,
                             zlib.crc32(body), b'LSMEND01')


def test_sorted_table_exact_format_and_tombstones():
    entries = {b'z': (1, 1, b''), b'': (2, 1, b'value'), b'k': (3, 2, b'')}
    encoded = encode(entries)
    assert encoded == raw([(b'', 2, 1, b'value'), (b'k', 3, 2, b''), (b'z', 1, 1, b'')])
    table = decode(encoded)
    assert table.entries == entries and (table.min_seq, table.max_seq) == (1, 3)
    assert table.size == len(encoded)


def test_value_byte_corruption_is_rejected_without_structural_changes():
    key, value = b'key', b'payload'
    entries = {key: (1, 1, value)}
    original = encode(entries)
    assert decode(original).entries == entries
    value_start = len(HEADER) + ENTRY.size + len(key)
    value_end = value_start + len(value)
    damaged = bytearray(original)
    damaged[value_start + 3] ^= 1
    assert len(damaged) == len(original)
    assert damaged[:value_start] == original[:value_start]
    assert damaged[value_end:] == original[value_end:]
    assert damaged[value_start:value_end] != value
    with pytest.raises(CorruptionError, match='checksum'):
        decode(bytes(damaged))


@pytest.mark.parametrize('kind', ['header_magic', 'version', 'footer_magic', 'checksum', 'truncated', 'trailing'])
def test_incomplete_or_corrupt_table(kind):
    data = bytearray(encode({b'k': (1, 1, b'v')}))
    if kind == 'header_magic': data[0] ^= 1
    elif kind == 'version': data[8] ^= 1
    elif kind == 'footer_magic': data[-1] ^= 1
    elif kind == 'checksum': data[20] ^= 1
    elif kind == 'truncated': data = data[:-1]
    else: data += b'!'
    with pytest.raises(CorruptionError): decode(bytes(data))


@pytest.mark.parametrize('entries,overrides', [
    ([(b'b', 1, 1, b''), (b'a', 2, 1, b'')], {}),
    ([(b'a', 1, 1, b''), (b'a', 2, 1, b'')], {}),
    ([(b'a', 1, 1, b''), (b'b', 1, 1, b'')], {}),
    ([(b'a', 0, 1, b'')], {}), ([(b'a', 1, 9, b'')], {}),
    ([(b'a', 1, 2, b'value')], {}),
    ([(b'a', 1, 1, b'')], {'count': 2}),
    ([(b'a', 1, 1, b'')], {'minimum': 0}),
    ([(b'a', 1, 1, b'')], {'maximum': 2}),
])
def test_checksummed_invalid_structure(entries, overrides):
    with pytest.raises(CorruptionError): decode(raw(entries, **overrides))


def test_truncated_entry_with_valid_checksum():
    body = HEADER + struct.pack('<IIQB', 100, 0, 1, 1) + b'short'
    data = body + FOOTER.pack(1, 1, 1, zlib.crc32(body), b'LSMEND01')
    with pytest.raises(CorruptionError): decode(data)


def test_empty_table_is_never_emitted():
    with pytest.raises(ValueError): encode({})
    body = HEADER
    with pytest.raises(CorruptionError):
        decode(body + FOOTER.pack(0, 0, 0, zlib.crc32(body), b'LSMEND01'))
