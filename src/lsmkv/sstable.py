"""Immutable version-1 sorted tables; decode validates the complete file."""
from dataclasses import dataclass
import struct
import zlib

from .errors import CorruptionError
from .wal import MAX_KEY, MAX_VALUE, MAX_SEQ, validate_key

HEADER = b'LSMSST01' + struct.pack('<I', 1)
FOOTER = struct.Struct('<QQQI8s')
ENTRY = struct.Struct('<IIQB')
END_MAGIC = b'LSMEND01'


@dataclass(frozen=True)
class Table:
    entries: dict[bytes, tuple[int, int, bytes]]
    min_seq: int
    max_seq: int
    size: int


def encode(entries):
    if not entries:
        raise ValueError('empty flush does not produce an SSTable')
    pieces = [HEADER]
    sequences = set()
    for key in sorted(entries):
        seq, op, value = entries[key]
        validate_key(key)
        if not isinstance(value, bytes):
            raise TypeError('value must be bytes')
        if (len(value) > MAX_VALUE or not 1 <= seq <= MAX_SEQ or op not in (1, 2)
                or (op == 2 and value) or seq in sequences):
            raise ValueError('invalid SSTable entry')
        sequences.add(seq)
        pieces.append(ENTRY.pack(len(key), len(value), seq, op) + key + value)
    body = b''.join(pieces)
    return body + FOOTER.pack(len(entries), min(sequences), max(sequences), zlib.crc32(body), END_MAGIC)


def decode(data):
    footer_offset = len(data) - FOOTER.size
    if footer_offset < len(HEADER) or data[:len(HEADER)] != HEADER:
        raise CorruptionError('invalid SSTable header or incomplete footer')
    count, minimum, maximum, crc, magic = FOOTER.unpack_from(data, footer_offset)
    if magic != END_MAGIC or zlib.crc32(data[:footer_offset]) != crc:
        raise CorruptionError('invalid SSTable footer or data checksum')
    position = len(HEADER)
    entries = {}
    sequences = set()
    previous = None
    while position < footer_offset:
        if footer_offset - position < ENTRY.size:
            raise CorruptionError('truncated SSTable entry header')
        klen, vlen, seq, op = ENTRY.unpack_from(data, position)
        position += ENTRY.size
        end = position + klen + vlen
        if (klen > MAX_KEY or vlen > MAX_VALUE or end > footer_offset
                or seq == 0 or op not in (1, 2) or (op == 2 and vlen) or seq in sequences):
            raise CorruptionError('invalid SSTable entry')
        key, value = data[position:position + klen], data[position + klen:end]
        if previous is not None and key <= previous:
            raise CorruptionError('SSTable keys are not strictly ascending')
        entries[key] = (seq, op, value)
        sequences.add(seq)
        previous = key
        position = end
    if (not entries or count != len(entries) or minimum != min(sequences)
            or maximum != max(sequences)):
        raise CorruptionError('SSTable footer count or sequence bounds mismatch')
    return Table(entries, minimum, maximum, len(data))
