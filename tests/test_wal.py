import struct
import zlib

import pytest

from lsmkv.errors import CorruptionError
from lsmkv.wal import (HEADER, MAX_KEY, MAX_VALUE, MAX_PAYLOAD, Record,
                       encode_record, replay)


def framed(payload):
    length = struct.pack('<I', len(payload))
    return length + struct.pack('<II', zlib.crc32(length), zlib.crc32(payload)) + payload


def payload(seq=1, op=1, key=b'k', value=b'v'):
    return struct.pack('<QBI', seq, op, len(key)) + key + struct.pack('<I', len(value)) + value


def test_wire_format_and_empty_values():
    assert HEADER == b'LSMWAL01' + struct.pack('<I', 1)
    for record in (Record(1, 1, b'k', b'v'), Record(2, 1, b'', b''), Record(3, 2, b'k', b'')):
        assert encode_record(record) == framed(payload(record.seq, record.op, record.key, record.value))
        result = replay(HEADER + encode_record(record))
        assert result.records == (record,)
        assert result.valid_end == len(HEADER) + len(encode_record(record))
        assert not result.incomplete


@pytest.mark.parametrize('op,key,value', [(1, b'', b''), (1, b'k', b'v'), (2, b'k', b'')])
def test_incomplete_final_record_at_every_byte(op, key, value):
    first = encode_record(Record(1, 1, b'old', b'yes'))
    final = encode_record(Record(2, op, key, value))
    for boundary in range(len(final)):
        result = replay(HEADER + first + final[:boundary])
        assert result.records == (Record(1, 1, b'old', b'yes'),)
        assert result.valid_end == len(HEADER) + len(first)
        assert result.incomplete is (boundary != 0)
        if boundary:
            with pytest.raises(CorruptionError):
                replay(HEADER + first + final[:boundary], last=False)
    assert len(replay(HEADER + first + final).records) == 2


def test_short_file_headers_and_invalid_complete_headers():
    for size in range(len(HEADER)):
        result = replay(b'?' * size)
        assert result.short_header and result.records == ()
        with pytest.raises(CorruptionError): replay(b'?' * size, last=False)
    for data in (b'?' + HEADER[1:], HEADER[:8] + struct.pack('<I', 2)):
        with pytest.raises(CorruptionError): replay(data)


@pytest.mark.parametrize('position', [0, 1, 2])
@pytest.mark.parametrize('field', [0, 4, 8, 12])
def test_corrupt_record_fields_in_first_middle_last(position, field):
    records = [bytearray(encode_record(Record(i + 1, 1, b'k', b'v'))) for i in range(3)]
    records[position][field] ^= 1
    with pytest.raises(CorruptionError): replay(HEADER + b''.join(records))


@pytest.mark.parametrize('length', [0, 1, 16, MAX_PAYLOAD + 1, 0xffffffff])
def test_invalid_declared_lengths_rejected_even_if_payload_missing(length):
    length_bytes = struct.pack('<I', length)
    with pytest.raises(CorruptionError):
        replay(HEADER + length_bytes + struct.pack('<II', zlib.crc32(length_bytes), 0))


@pytest.mark.parametrize('bad', [
    payload(seq=0), payload(op=0), payload(op=3), payload(op=2, value=b'not-empty'),
    payload() + b'extra', payload()[:-1],
    struct.pack('<QBI', 1, 1, MAX_KEY + 1) + b'x' * 4,
    struct.pack('<QBI', 1, 1, 0) + struct.pack('<I', MAX_VALUE + 1),
])
def test_checksummed_but_malformed_payload(bad):
    with pytest.raises(CorruptionError): replay(HEADER + framed(bad))


@pytest.mark.parametrize('sequences', [(1, 1), (2, 1), (4, 3)])
def test_sequence_ordering(sequences):
    with pytest.raises(CorruptionError):
        replay(HEADER + b''.join(encode_record(Record(seq, 1, b'k', b'v')) for seq in sequences))
    with pytest.raises(CorruptionError): replay(HEADER + encode_record(Record(2, 1, b'k', b'v')), previous_seq=2)


def test_zero_filled_tail_is_corruption_at_full_record_header():
    with pytest.raises(CorruptionError): replay(HEADER + b'\0' * 12)


def test_codec_rejects_invalid_input_before_encoding():
    for record in (Record(0, 1, b'', b''), Record(1 << 64, 1, b'', b''),
                   Record(1, 3, b'', b''), Record(1, 2, b'', b'x'),
                   Record(1, 1, b'k' * (MAX_KEY + 1), b'')):
        with pytest.raises(ValueError): encode_record(record)
