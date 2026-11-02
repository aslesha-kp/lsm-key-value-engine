import json
import struct
import zlib

import pytest

from lsmkv.errors import CorruptionError, UnsupportedFormatError
from lsmkv.manifest import INITIAL, encode_initial, validate_m1


def frame(payload, version=1):
    fields = struct.pack('<II', version, len(payload))
    return b'LSMMAN01' + fields + struct.pack('<I', zlib.crc32(fields + payload)) + payload


def test_initial_manifest_uses_final_layout_and_checksum_coverage():
    encoded = encode_initial()
    magic, version, length, crc = struct.unpack('<8sIII', encoded[:20])
    assert magic == b'LSMMAN01' and version == 1
    assert length == len(encoded) - 20
    assert crc == zlib.crc32(encoded[8:16] + encoded[20:])
    assert json.loads(encoded[20:]) == INITIAL
    assert validate_m1(encoded) == INITIAL


@pytest.mark.parametrize('change', ['magic', 'version', 'length', 'crc', 'payload', 'truncated', 'extra'])
def test_corrupt_manifest_is_fatal(change):
    data = bytearray(encode_initial())
    positions = {'magic': 0, 'version': 8, 'length': 12, 'crc': 16, 'payload': 20}
    if change in positions: data[positions[change]] ^= 1
    elif change == 'truncated': data = data[:-1]
    else: data += b'!'
    with pytest.raises(CorruptionError): validate_m1(bytes(data))


@pytest.mark.parametrize('payload', [b'{', b'[]', b'{}', b'{"generation":0,"generation":1}',
    b'{"generation":true,"flushed_seq":0,"next_file_number":2,"sstables":[]}',
    b'{"generation":0,"flushed_seq":0,"next_file_number":0,"sstables":[]}'])
def test_invalid_manifest_schema(payload):
    with pytest.raises(CorruptionError): validate_m1(frame(payload))


@pytest.mark.parametrize('changes', [{'generation': 1}, {'flushed_seq': 1},
                                   {'sstables': [{'name': 'sst-000002.sst'}]}])
def test_later_milestones_rejected_without_interpretation(changes):
    metadata = {**INITIAL, **changes}
    with pytest.raises(UnsupportedFormatError):
        validate_m1(frame(json.dumps(metadata).encode()))


def full_metadata():
    return {'generation': 2, 'flushed_seq': 5, 'next_file_number': 7,
            'sstables': [{'name': 'sst-000003.sst', 'size': 90, 'min_seq': 1, 'max_seq': 2},
                         {'name': 'sst-000005.sst', 'size': 90, 'min_seq': 4, 'max_seq': 5}]}


def test_full_manifest_roundtrip():
    from lsmkv.manifest import encode, decode
    metadata = full_metadata()
    assert decode(encode(metadata)) == metadata
    assert decode(frame(json.dumps(metadata).encode())) == metadata


@pytest.mark.parametrize('damage', ['overlap', 'reversed', 'duplicate', 'name', 'size', 'sequence',
    'watermark', 'counter', 'bool', 'unknown', 'missing', 'number_order', 'zero_generation', 'counter_overflow'])
def test_full_manifest_rejects_invalid_metadata(damage):
    from lsmkv.manifest import decode
    m = full_metadata()
    if damage == 'overlap': m['sstables'][1]['min_seq'] = 2
    elif damage == 'reversed': m['sstables'].reverse()
    elif damage == 'duplicate': m['sstables'][1]['name'] = m['sstables'][0]['name']
    elif damage == 'name': m['sstables'][0]['name'] = '../sst-000003.sst'
    elif damage == 'size': m['sstables'][0]['size'] = 64
    elif damage == 'sequence': m['sstables'][0]['min_seq'] = 0
    elif damage == 'watermark': m['flushed_seq'] = 4
    elif damage == 'counter': m['next_file_number'] = 5
    elif damage == 'bool': m['sstables'][0]['size'] = True
    elif damage == 'unknown': m['sstables'][0]['unexpected'] = 1
    elif damage == 'missing': del m['sstables'][0]['size']
    elif damage == 'number_order': m['sstables'][0]['name'] = 'sst-000006.sst'
    elif damage == 'zero_generation': m['generation'] = 0
    else: m['next_file_number'] = 1 << 64
    with pytest.raises(CorruptionError): decode(frame(json.dumps(m).encode()))
