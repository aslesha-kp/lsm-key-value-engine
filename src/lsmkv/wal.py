"""Version-1 WAL wire format and conservative, side-effect-free replay."""
from dataclasses import dataclass
import struct
import zlib

from .errors import CorruptionError

HEADER = b'LSMWAL01' + struct.pack('<I', 1)
MAX_KEY = 64 * 1024
MAX_VALUE = 16 * 1024 * 1024
MIN_PAYLOAD = 17
MAX_PAYLOAD = MIN_PAYLOAD + MAX_KEY + MAX_VALUE
MAX_SEQ = (1 << 64) - 1


@dataclass(frozen=True)
class Record:
    seq: int
    op: int
    key: bytes
    value: bytes


@dataclass(frozen=True)
class Replay:
    records: tuple[Record, ...]
    valid_end: int
    incomplete: bool = False
    short_header: bool = False


def validate_key(key):
    if not isinstance(key, bytes):
        raise TypeError('key must be bytes')
    if len(key) > MAX_KEY:
        raise ValueError('key exceeds 64 KiB')


def encode_record(record):
    validate_key(record.key)
    if not isinstance(record.value, bytes):
        raise TypeError('value must be bytes')
    if len(record.value) > MAX_VALUE:
        raise ValueError('value exceeds 16 MiB')
    if not 1 <= record.seq <= MAX_SEQ or record.op not in (1, 2):
        raise ValueError('invalid sequence or operation')
    if record.op == 2 and record.value:
        raise ValueError('delete must have an empty value')
    payload = (struct.pack('<QBI', record.seq, record.op, len(record.key)) + record.key
               + struct.pack('<I', len(record.value)) + record.value)
    length = struct.pack('<I', len(payload))
    return length + struct.pack('<II', zlib.crc32(length), zlib.crc32(payload)) + payload


def _decode_payload(payload):
    seq, op, klen = struct.unpack_from('<QBI', payload)
    if seq == 0 or op not in (1, 2) or klen > MAX_KEY or 13 + klen + 4 > len(payload):
        raise CorruptionError('invalid WAL sequence, operation, or key length')
    vlen = struct.unpack_from('<I', payload, 13 + klen)[0]
    if vlen > MAX_VALUE or 17 + klen + vlen != len(payload) or (op == 2 and vlen != 0):
        raise CorruptionError('invalid WAL value length or delete payload')
    return Record(seq, op, payload[13:13 + klen], payload[17 + klen:])


def replay(data, *, last=True, previous_seq=0):
    if len(data) < len(HEADER):
        if not last:
            raise CorruptionError('short header in non-last WAL')
        return Replay((), 0, short_header=True)
    if data[:len(HEADER)] != HEADER:
        raise CorruptionError('invalid WAL magic or version')
    position = len(HEADER)
    records = []
    while position < len(data):
        remaining = len(data) - position
        if remaining < 12:
            if not last:
                raise CorruptionError('incomplete header in non-last WAL')
            return Replay(tuple(records), position, incomplete=True)
        length_bytes = data[position:position + 4]
        length, header_crc, payload_crc = struct.unpack_from('<III', data, position)
        if zlib.crc32(length_bytes) != header_crc:
            raise CorruptionError('WAL header checksum mismatch')
        # Only after checking header_crc can the declared length be trusted.
        if not MIN_PAYLOAD <= length <= MAX_PAYLOAD:
            raise CorruptionError('illegal WAL payload length')
        end = position + 12 + length
        if end > len(data):
            if not last:
                raise CorruptionError('incomplete payload in non-last WAL')
            return Replay(tuple(records), position, incomplete=True)
        payload = data[position + 12:end]
        if zlib.crc32(payload) != payload_crc:
            raise CorruptionError('WAL payload checksum mismatch')
        record = _decode_payload(payload)
        if record.seq <= previous_seq:
            raise CorruptionError('WAL sequences must be strictly increasing')
        previous_seq = record.seq
        records.append(record)
        position = end
    return Replay(tuple(records), position)
