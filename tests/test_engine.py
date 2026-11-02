from copy import deepcopy
import struct
import zlib

import pytest

from lsmkv.engine import Engine
from lsmkv.errors import (CorruptionError, MissingManifestError, UnsupportedFormatError,
                          FailedStateError, ClosedError)
from lsmkv.harness import FaultFS, FaultPlan, CrashCase, InjectedCrash, run_crashes
from lsmkv.model import History, Operation, generate_operations
from lsmkv.simfs import SimFS, CrashChoice
from lsmkv.wal import HEADER, MAX_KEY, MAX_VALUE, MAX_SEQ, Record, encode_record

WAL = 'wal-000001.log'


def initialized():
    fs = SimFS()
    db = Engine.open(fs)
    db.close(); fs.reopen()
    return fs


def contents(db, keys):
    # Whole-state checks must not project onto caller-supplied known keys.
    return db.snapshot()


def recover_state(io, keys):
    db = Engine.open(io)
    state = contents(db, keys)
    db.close()
    return state


def replace_bytes(fs, name, data):
    fs.truncate(name, 0); fs.write(name, data); fs.sync_file(name); fs.sync_dir()


def test_operations_empty_keys_values_and_sequence_recovery():
    fs = SimFS(directory_exists=False)
    db = Engine.open(fs)
    assert set(fs.listdir()) == {'LOCK', 'MANIFEST', WAL}
    assert db.next_seq == 1
    db.put(b'', b''); db.put(b'k', b'old'); db.put(b'k', b'new')
    db.delete(b'k'); db.delete(b'absent')
    assert db.get(b'') == b'' and db.get(b'k') is None
    assert db.next_seq == 6
    db.close()
    with pytest.raises(ClosedError): db.get(b'')
    db = Engine.open(fs)
    assert db.next_seq == 6
    assert contents(db, [b'', b'k', b'absent']) == {b'': b''}
    db.put(b'k', b'reborn'); db.close(); fs.crash()
    db = Engine.open(fs)
    assert db.next_seq == 7 and db.get(b'k') == b'reborn'
    db.close()


def test_input_limits_before_storage_and_exact_limits_are_accepted():
    fs = initialized(); io = FaultFS(fs); db = Engine.open(io)
    before_calls, before_bytes = io.calls, fs.read(WAL)
    for action in [lambda: db.put(b'k' * (MAX_KEY + 1), b'v'),
                   lambda: db.put(b'k', b'v' * (MAX_VALUE + 1)),
                   lambda: db.get(b'k' * (MAX_KEY + 1)),
                   lambda: db.delete(b'k' * (MAX_KEY + 1))]:
        with pytest.raises(ValueError): action()
    for action in [lambda: db.put('k', b'v'), lambda: db.put(b'k', bytearray(b'v')),
                   lambda: db.get(None), lambda: db.delete('k')]:
        with pytest.raises(TypeError): action()
    assert io.calls == before_calls and fs.read(WAL) == before_bytes
    assert db.next_seq == 1 and not db.failed
    key, value = b'k' * MAX_KEY, b'v' * MAX_VALUE
    db.put(key, value)
    assert db.get(key) == value
    db.close(); fs.crash(); db = Engine.open(fs)
    assert db.get(key) == value
    db.delete(key); assert db.get(key) is None; db.close()


def test_second_opener_does_not_unlock_first_owner():
    fs = SimFS(); first = Engine.open(fs)
    with pytest.raises(BlockingIOError): Engine.open(fs)
    first.put(b'k', b'v')
    with pytest.raises(BlockingIOError): Engine.open(fs)
    first.close(); second = Engine.open(fs)
    assert second.get(b'k') == b'v'; second.close()


def test_acknowledged_wal_append_survives_every_crash_choice():
    fs = initialized(); db = Engine.open(fs)
    db.put(b'k', b'acknowledged')
    h = History(); h.begin(Operation('put', b'k', b'acknowledged')); h.acknowledge()
    for choice in fs.enumerate_crashes():
        trial = deepcopy(fs); trial.crash(choice)
        h.check(recover_state(trial, [b'k']))
    db.close()


def workload_case(operations):
    def factory():
        fs = initialized(); h = History()
        keys = {op.key for op in operations}
        def workload(io):
            db = Engine.open(io)
            for op in operations:
                h.begin(op)
                if op.kind == 'put': db.put(op.key, op.value)
                elif op.kind == 'delete': db.delete(op.key)
                else: assert db.get(op.key) == h.state.get(op.key)
                h.acknowledge()
            db.close()
        return CrashCase(fs, workload, lambda io: recover_state(io, keys),
                         lambda state, event: h.check(state))
    return factory


def test_exhaustive_call_crashes_overwrite_delete_empty_values():
    operations = [Operation('put', b'', b''), Operation('put', b'k', b'1'),
                  Operation('put', b'k', b'2'), Operation('delete', b'k')]
    reports = run_crashes(workload_case(operations))
    assert any(r.event.method == 'write' for r in reports)
    assert any(r.event.phase == 'complete' for r in reports)
    assert all(r.seed is None for r in reports)


@pytest.mark.parametrize('seed', [0, 1, 7, 42, 2026])
def test_seeded_reference_workload_and_sampled_crashes(seed):
    operations = list(generate_operations(seed, 30, key_count=4))
    reports = run_crashes(workload_case(operations), seeds=(seed, seed + 1000))
    assert {r.seed for r in reports} == {seed, seed + 1000}


@pytest.mark.parametrize('op,key,value', [(1, b'', b''), (1, b'k', b'v'), (2, b'k', b'')])
def test_engine_repairs_every_incomplete_final_record(op, key, value):
    first = encode_record(Record(1, 1, b'k', b'old'))
    tail = encode_record(Record(2, op, key, value))
    for boundary in range(len(tail)):
        fs = initialized(); replace_bytes(fs, WAL, HEADER + first + tail[:boundary])
        db = Engine.open(fs)
        assert db.get(b'k') == b'old'
        assert db.next_seq == 2
        assert fs.read(WAL) == HEADER + first
        db.put(b'z', b'next'); db.close(); fs.crash()
        db = Engine.open(fs)
        assert db.next_seq == 3 and db.get(b'z') == b'next'; db.close()


def test_nonlast_wal_incomplete_is_fatal_and_order_is_global():
    fs = initialized(); fs.create('wal-000002.log')
    fs.write('wal-000002.log', HEADER); fs.sync_file('wal-000002.log'); fs.sync_dir()
    replace_bytes(fs, WAL, HEADER + encode_record(Record(1, 1, b'k', b'v'))[:-1])
    before = fs.read(WAL)
    with pytest.raises(CorruptionError): Engine.open(fs)
    assert fs.read(WAL) == before
    replace_bytes(fs, WAL, HEADER + encode_record(Record(2, 1, b'k', b'v')))
    replace_bytes(fs, 'wal-000002.log', HEADER + encode_record(Record(1, 1, b'k', b'old')))
    with pytest.raises(CorruptionError): Engine.open(fs)


def test_sequence_gaps_and_exhaustion():
    fs = initialized()
    replace_bytes(fs, WAL, HEADER + encode_record(Record(MAX_SEQ, 1, b'k', b'last')))
    db = Engine.open(fs)
    assert db.next_seq == MAX_SEQ + 1
    before = fs.read(WAL)
    with pytest.raises(OverflowError): db.put(b'k', b'overflow')
    assert fs.read(WAL) == before and not db.failed
    db.close()


@pytest.mark.parametrize('phase', ['before', 'after'])
@pytest.mark.parametrize('method', ['write', 'sync_file'])
def test_write_sync_errors_fail_closed_without_retry(method, phase):
    fs = initialized(); io = FaultFS(fs); db = Engine.open(io)
    db.put(b'base', b'good')
    io.plan = FaultPlan(io.calls + (method == 'sync_file'), phase, 'error')
    with pytest.raises(OSError): db.put(b'k', b'new')
    assert db.failed and b'k' not in db.memtable
    calls = io.calls
    for action in [lambda: db.get(b'base'), lambda: db.get(b'k'),
                   lambda: db.put(b'k', b'retry'), lambda: db.delete(b'base')]:
        with pytest.raises(FailedStateError): action()
    assert io.calls == calls
    db.close(); fs.crash()
    db = Engine.open(fs)
    assert db.get(b'base') == b'good'
    assert db.get(b'k') in (None, b'new'); db.close()


def test_partial_write_errors_at_every_byte_and_reopen_without_crash():
    data = encode_record(Record(2, 1, b'k', b'new'))
    for count in range(len(data) + 1):
        fs = initialized(); io = FaultFS(fs); db = Engine.open(io)
        db.put(b'base', b'good')
        io.plan = FaultPlan(io.calls, 'before', 'error', write_prefix=count)
        with pytest.raises(OSError): db.put(b'k', b'new')
        assert db.failed and b'k' not in db.memtable
        db.close()
        db = Engine.open(fs)  # Pending bytes remain visible: no simulated crash.
        assert db.get(b'base') == b'good'
        assert db.get(b'k') == (b'new' if count == len(data) else None)
        assert db.next_seq == (3 if count == len(data) else 2)
        db.put(b'after', b'ok'); db.close(); fs.crash()
        db = Engine.open(fs); assert db.get(b'after') == b'ok'; db.close()


def test_interrupted_initialization_variants_and_missing_manifest_fail_closed():
    for size in range(len(HEADER) + 1):
        fs = SimFS(); fs.create(WAL); fs.write(WAL, HEADER[:size]); fs.sync_file(WAL); fs.sync_dir()
        db = Engine.open(fs)
        assert fs.read(WAL) == HEADER and db.next_seq == 1
        db.put(b'k', b'v'); db.close(); fs.crash()
        db = Engine.open(fs); assert db.get(b'k') == b'v'; db.close()
    for wal in [b'bad-header!!', HEADER + encode_record(Record(1, 1, b'k', b'v'))]:
        fs = SimFS(); fs.create(WAL); fs.write(WAL, wal); fs.sync_file(WAL); fs.sync_dir()
        fs.create('unrelated'); fs.write('unrelated', b'keep')
        before = {name: fs.read(name) for name in fs.listdir()}
        with pytest.raises(MissingManifestError): Engine.open(fs)
        assert all(fs.read(name) == data for name, data in before.items())
        assert set(before) <= set(fs.listdir())


def test_missing_wal_is_fatal_at_generation_zero():
    fs = initialized(); fs.unlink(WAL); fs.sync_dir()
    with pytest.raises(CorruptionError, match='WAL'): Engine.open(fs)
    assert WAL not in fs.listdir()


def test_missing_manifest_with_only_synced_sstable_preserves_file():
    from lsmkv.sstable import encode as encode_table
    fs = SimFS()
    name = 'sst-000002.sst'
    data = encode_table({b'key': (1, 1, b'value')})
    fs.create(name)
    fs.write(name, data)
    fs.sync_file(name)
    fs.sync_dir()
    fs.crash()
    assert fs.listdir() == [name]
    assert fs.read(name) == data
    db = None
    try:
        with pytest.raises(MissingManifestError):
            db = Engine.open(fs)
    finally:
        if db is not None:
            db.close()
    assert fs.read(name) == data
    names = set(fs.listdir())
    assert 'MANIFEST' not in names
    assert not any(entry.startswith('wal-') for entry in names)
    assert names <= {name, 'LOCK'}
    fs.crash()
    assert fs.read(name) == data


def test_existing_short_header_is_repaired_and_synced_before_append():
    for size in range(len(HEADER)):
        fs = initialized(); replace_bytes(fs, WAL, b'?' * size)
        db = Engine.open(fs)
        assert fs.read(WAL) == HEADER
        db.put(b'k', b'v'); db.close(); fs.crash()
        db = Engine.open(fs); assert db.get(b'k') == b'v'; db.close()


def test_corruption_does_not_cleanup_files_and_open_releases_its_lock():
    fs = initialized(); fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', b'keep')
    original = fs.read(WAL)
    replace_bytes(fs, WAL, b'?' + HEADER[1:])
    with pytest.raises(CorruptionError): Engine.open(fs)
    assert fs.read('MANIFEST.tmp') == b'keep'
    replace_bytes(fs, WAL, original)
    db = Engine.open(fs); db.close()


@pytest.mark.parametrize('operation', ['sync_file', 'sync_dir', 'sync_parent'])
@pytest.mark.parametrize('phase', ['before', 'after'])
def test_initialization_sync_errors_abort_and_reopen(operation, phase):
    fs = SimFS(directory_exists=False)
    trace = FaultFS(deepcopy(fs))
    Engine.open(trace)
    target = next(event.call for event in trace.events if event.method == operation)
    io = FaultFS(fs, FaultPlan(target, phase, 'error'))
    with pytest.raises(OSError): Engine.open(io)
    # No usable engine was returned. Reopen with no crash or with default loss.
    for crash in (False, True):
        trial = deepcopy(fs)
        if crash: trial.crash()
        db = Engine.open(trial); db.put(b'k', b'v'); db.close(); trial.crash()
        db = Engine.open(trial); assert db.get(b'k') == b'v'; db.close()


def test_multiple_wals_recover_highest_sequence_and_short_last_header():
    fs = initialized()
    replace_bytes(fs, WAL, HEADER + encode_record(Record(7, 1, b'k', b'old')))
    fs.create('wal-000002.log'); fs.write('wal-000002.log', HEADER[:4])
    fs.sync_file('wal-000002.log'); fs.sync_dir()
    db = Engine.open(fs)
    assert db.next_seq == 8 and db.get(b'k') == b'old'
    db.delete(b'k'); db.put(b'z', b'v'); db.close(); fs.crash()
    db = Engine.open(fs)
    assert db.next_seq == 10 and db.get(b'k') is None and db.get(b'z') == b'v'
    db.close()


def test_missing_manifest_with_invalid_or_nonempty_only_wal_deletes_nothing():
    for data in (b'BADMAGIC' + HEADER[8:], HEADER + b'x',
                 HEADER + encode_record(Record(1, 1, b'k', b'v'))):
        fs = SimFS(); fs.create(WAL); fs.write(WAL, data); fs.sync_file(WAL); fs.sync_dir()
        with pytest.raises(MissingManifestError): Engine.open(fs)
        assert fs.read(WAL) == data and 'MANIFEST' not in fs.listdir()


def test_corrupt_or_unsupported_manifest_does_not_remove_anything():
    import json
    fs = initialized(); db = Engine.open(fs); db.put(b'k', b'ack'); db.close(); fs.reopen()
    original = fs.read('MANIFEST')
    fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', b'keep')
    broken = bytearray(original); broken[-1] ^= 1
    for data, error in [(bytes(broken), CorruptionError)]:
        replace_bytes(fs, 'MANIFEST', data)
        before = {name: fs.read(name) for name in fs.listdir()}
        with pytest.raises(error): Engine.open(fs)
        assert {name: fs.read(name) for name in fs.listdir()} == before
    metadata = json.loads(original[20:]); metadata['generation'] = 1
    payload = json.dumps(metadata).encode(); fields = struct.pack('<II', 1, len(payload))
    replace_bytes(fs, 'MANIFEST', b'LSMMAN01' + fields + struct.pack('<I', zlib.crc32(fields + payload)) + payload)
    before = {name: fs.read(name) for name in fs.listdir()}
    with pytest.raises(UnsupportedFormatError): Engine.open(fs)
    assert {name: fs.read(name) for name in fs.listdir()} == before


def test_failed_sync_complete_record_is_stabilized_on_reopen_without_crash():
    fs = initialized(); io = FaultFS(fs); db = Engine.open(io)
    db.put(b'base', b'ack')
    io.plan = FaultPlan(io.calls + 1, 'before', 'error')
    with pytest.raises(OSError): db.put(b'k', b'unacknowledged')
    db.close()
    db = Engine.open(fs)
    assert db.get(b'k') == b'unacknowledged'
    # Successful recovery has selected the complete in-flight operation. Its
    # returned image must now survive another crash even without a new write.
    db.close(); fs.crash()
    assert recover_state(fs, [b'base', b'k']) == {b'base': b'ack', b'k': b'unacknowledged'}
