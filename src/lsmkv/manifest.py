"""Version-1 manifest framing, complete metadata validation, and M1 compatibility."""
import json
import re
import struct
import zlib

from .errors import CorruptionError, UnsupportedFormatError
from .wal import MAX_SEQ

MAGIC = b'LSMMAN01'
VERSION = 1
INITIAL = {'generation': 0, 'flushed_seq': 0, 'next_file_number': 2, 'sstables': []}


SST_NAME = re.compile(r'sst-([0-9]{6,})\.sst')
NUMBERED = re.compile(r'(?:wal-([0-9]{6,})\.log|sst-([0-9]{6,})\.sst)(?:\.tmp)?')


def file_number(name):
    match = NUMBERED.fullmatch(name)
    if not match:
        return None
    digits = match.group(1) or match.group(2)
    number = int(digits)
    return number if number > 0 and digits == f'{number:06d}' else None


def encode_initial():
    return encode(INITIAL)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate manifest key')
        result[key] = value
    return result


def _decode_fields(data):
    if len(data) < 20:
        raise CorruptionError('short manifest header')
    magic, version, length, crc = struct.unpack_from('<8sIII', data)
    if magic != MAGIC or version != VERSION or len(data) != 20 + length:
        raise CorruptionError('invalid manifest magic, version, or length')
    if zlib.crc32(data[8:16] + data[20:]) != crc:
        raise CorruptionError('manifest checksum mismatch')
    try:
        metadata = json.loads(data[20:].decode('utf-8'), object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError) as error:
        raise CorruptionError('invalid manifest JSON') from error
    if not isinstance(metadata, dict) or set(metadata) != set(INITIAL):
        raise CorruptionError('invalid manifest fields')
    for field in ('generation', 'flushed_seq', 'next_file_number'):
        if type(metadata[field]) is not int or metadata[field] < 0:
            raise CorruptionError('invalid manifest integer')
    if metadata['next_file_number'] < 2 or not isinstance(metadata['sstables'], list):
        raise CorruptionError('invalid manifest allocation or SSTable list')
    return metadata


def validate_m1(data):
    """Retained M1 subset validator; the M2 engine uses decode instead."""
    metadata = _decode_fields(data)
    if metadata['generation'] != 0 or metadata['flushed_seq'] != 0 or metadata['sstables']:
        raise UnsupportedFormatError('M1 supports only generation 0 without SSTables')
    return metadata


def _validate(metadata):
    if not isinstance(metadata, dict) or set(metadata) != set(INITIAL):
        raise CorruptionError('invalid manifest fields')
    for field in ('generation', 'flushed_seq', 'next_file_number'):
        if type(metadata[field]) is not int or not 0 <= metadata[field] <= MAX_SEQ:
            raise CorruptionError('invalid manifest integer')
    if metadata['next_file_number'] < 2 or not isinstance(metadata['sstables'], list):
        raise CorruptionError('invalid manifest allocation or SSTable list')
    if metadata['generation'] == 0 and (metadata['flushed_seq'] or metadata['sstables']):
        raise CorruptionError('generation zero must be unflushed')
    if metadata['generation'] > 0 and metadata['flushed_seq'] == 0:
        # Preserve M1 rejection of metadata no supported writer can produce.
        raise UnsupportedFormatError('noninitial generation without a flushed sequence')
    previous_seq, previous_number = 0, 0
    for table in metadata['sstables']:
        if not isinstance(table, dict) or set(table) != {'name', 'size', 'min_seq', 'max_seq'}:
            raise CorruptionError('invalid SSTable manifest fields')
        name = table['name']
        if not isinstance(name, str) or not SST_NAME.fullmatch(name) or file_number(name) is None:
            raise CorruptionError('invalid SSTable filename')
        number = file_number(name)
        if any(type(table[field]) is not int for field in ('size', 'min_seq', 'max_seq')):
            raise CorruptionError('invalid SSTable manifest integer')
        if (table['size'] < 65 or not previous_seq < table['min_seq'] <= table['max_seq']
                <= metadata['flushed_seq'] or not previous_number < number < metadata['next_file_number']):
            raise CorruptionError('invalid SSTable size, ordering, range, or allocation')
        previous_seq, previous_number = table['max_seq'], number
    return metadata


def encode(metadata):
    _validate(metadata)
    payload = json.dumps(metadata, sort_keys=True, separators=(',', ':')).encode('utf-8')
    fields = struct.pack('<II', VERSION, len(payload))
    return MAGIC + fields + struct.pack('<I', zlib.crc32(fields + payload)) + payload


def decode(data):
    return _validate(_decode_fields(data))
