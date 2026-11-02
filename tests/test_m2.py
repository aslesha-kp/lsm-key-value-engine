from copy import deepcopy

import pytest

from lsmkv import Engine, CorruptionError, FailedStateError
from lsmkv.manifest import encode, decode
from lsmkv.model import History, Operation, generate_operations
from lsmkv.simfs import SimFS
from lsmkv.harness import FaultFS, FaultPlan, CrashCase, run_crashes
from lsmkv.wal import HEADER, Record, encode_record
from test_engine import initialized, replace_bytes, recover_state, contents, WAL


def metadata(fs):
    return decode(fs.read('MANIFEST'))


def test_known_key_projection_cannot_hide_an_extra_recovered_key():
    fs = SimFS(); db = Engine.open(fs)
    db.put(b'known', b'ok'); db.put(b'unexpected', b'extra')
    h = History(); h.begin(Operation('put', b'known', b'ok')); h.acknowledge()
    with pytest.raises(AssertionError): h.check(contents(db, [b'known']))
    db.close()


def test_flush_order_reads_tombstones_and_sequence_recovery():
    fs = SimFS(); db = Engine.open(fs)
    db.put(b'k', b'old'); db.put(b'', b''); db.flush()
    first = metadata(fs)
    assert first['generation'] == 1 and first['flushed_seq'] == 2
    assert first['next_file_number'] == 4
    assert [s['name'] for s in first['sstables']] == ['sst-000003.sst']
    assert [name for name in fs.listdir() if name.startswith('wal-')] == ['wal-000002.log']
    db.delete(b'k'); db.put(b'z', b'v'); db.flush()
    db.put(b'k', b'new')
    assert db.snapshot() == {b'': b'', b'z': b'v', b'k': b'new'}
    assert db.get(b'k') == b'new'
    assert len(metadata(fs)['sstables']) == 2
    db.close(); fs.crash(); db = Engine.open(fs)
    assert db.snapshot() == {b'': b'', b'z': b'v', b'k': b'new'}
    assert db.next_seq == 6
    db.delete(b'k'); assert db.get(b'k') is None
    db.flush(); db.close(); fs.crash()
    db = Engine.open(fs); assert db.snapshot() == {b'': b'', b'z': b'v'}; db.close()


def test_empty_flush_is_noop_and_triggers_account_live_bytes():
    fs = SimFS(); io = FaultFS(fs)
    db = Engine.open(io, memtable_bytes=20, wal_bytes=10000)
    before = io.calls
    db.flush(); assert io.calls == before
    db.put(b'a', b'x')  # 17 + key + value = 19 encoded live bytes
    assert metadata(fs)['generation'] == 0
    db.put(b'a', b'yy')  # replace rather than accumulate: 20 bytes
    assert metadata(fs)['generation'] == 1
    assert db.memtable == {} and db.snapshot() == {b'a': b'yy'}
    db.close()


def test_wal_trigger_overwrites_and_overshoot_bound():
    fs = SimFS(); db = Engine.open(fs, memtable_bytes=1000, wal_bytes=75)
    db.put(b'k', b'v'); db.put(b'k', b'w')
    assert metadata(fs)['generation'] == 0  # WAL header 12 + 2*31 = 74
    db.put(b'k', b'x')
    assert metadata(fs)['generation'] == 1 and metadata(fs)['flushed_seq'] == 3
    assert db.snapshot() == {b'k': b'x'}
    assert len(fs.read(db.active_wal)) == len(HEADER)
    assert db.last_flush_wal_bytes == 105
    assert db.last_flush_wal_bytes < 75 + 31
    db.close()


@pytest.mark.parametrize('orphan', ['sst-000100.sst', 'sst-000100.sst.tmp', 'wal-000100.log.tmp'])
def test_orphan_number_scan_before_cleanup_prevents_reuse(orphan):
    fs = initialized()
    fs.create(orphan); fs.write(orphan, b'orphan')
    fs.create('wal-000099.log'); fs.write('wal-000099.log', HEADER)
    fs.sync_file('wal-000099.log'); fs.sync_dir()
    db = Engine.open(fs)
    assert orphan not in fs.listdir()
    db.put(b'k', b'v'); db.flush()
    assert metadata(fs)['sstables'][0]['name'] == 'sst-000102.sst'
    assert db.active_wal == 'wal-000101.log'
    db.close()


def test_watermark_filters_retained_covered_records():
    fs = SimFS(); db = Engine.open(fs)
    db.put(b'k', b'old'); db.flush()
    db.delete(b'k'); db.flush()
    active = db.active_wal
    db.close(); fs.reopen()
    # Reappear as a covered old WAL after an unsynced retirement was lost.
    fs.create(WAL); fs.write(WAL, HEADER + encode_record(Record(1, 1, b'k', b'old')))
    fs.sync_file(WAL); fs.sync_dir()
    db = Engine.open(fs)
    assert db.memtable == {} and db.get(b'k') is None
    assert WAL not in fs.listdir() and active in fs.listdir()
    assert db.next_seq == 3
    db.close()


def test_validation_precedes_orphan_cleanup():
    fs = SimFS(); db = Engine.open(fs); db.put(b'k', b'v'); db.flush(); db.close(); fs.reopen()
    name = metadata(fs)['sstables'][0]['name']
    original = fs.read(name)
    fs.create('orphan.tmp'); fs.write('orphan.tmp', b'keep')
    for damage in (None, original[:-1], b'?' + original[1:]):
        if name in fs.listdir(): fs.unlink(name)
        if damage is not None: fs.create(name); fs.write(name, damage)
        fs.sync_dir()
        before = {n: fs.read(n) for n in fs.listdir()}
        with pytest.raises(CorruptionError): Engine.open(fs)
        assert {n: fs.read(n) for n in fs.listdir()} == before
    fs.unlink(name); fs.create(name); fs.write(name, original); fs.sync_file(name); fs.sync_dir()
    db = Engine.open(fs)
    assert 'orphan.tmp' not in fs.listdir() and db.get(b'k') == b'v'; db.close()


def flush_factory():
    fs = initialized(); db = Engine.open(fs); db.put(b'k', b'v'); db.close(); fs.reopen()
    h = History(); h.begin(Operation('put', b'k', b'v')); h.acknowledge()
    def workload(io):
        db = Engine.open(io); db.flush(); db.close()
    return CrashCase(fs, workload, lambda io: recover_state(io, [b'k']),
                     lambda image, event: h.check(image))


def test_flush_at_every_call_and_modeled_crash_outcome():
    reports = run_crashes(flush_factory)
    print(f'M2 tiny flush: {len(reports)} exhaustive boundary/persistence cases')
    assert any(r.event.method == 'unlink' for r in reports)


@pytest.mark.parametrize('seed', [3, 17, 42])
def test_seeded_multi_flush_reference_workloads(seed):
    operations = list(generate_operations(seed, 35, key_count=5))
    def factory():
        fs = initialized(); h = History()
        def workload(io):
            db = Engine.open(io, memtable_bytes=55, wal_bytes=120)
            for op in operations:
                h.begin(op)
                if op.kind == 'put': db.put(op.key, op.value)
                elif op.kind == 'delete': db.delete(op.key)
                else: assert db.get(op.key) == h.state.get(op.key)
                h.acknowledge()
                h.check(db.snapshot())
            db.close()
        return CrashCase(fs, workload, lambda io: recover_state(io, []),
                         lambda image, event: h.check(image))
    reports = run_crashes(factory, seeds=(seed, seed + 1000))
    assert {r.seed for r in reports} == {seed, seed + 1000}


@pytest.mark.parametrize('phase', ['before', 'after'])
def test_flush_io_errors_fail_closed_and_preserve_acknowledged_state(phase):
    original = flush_factory().fs
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); start = trace.calls
    db.flush(); db.close()
    targets = {(event.call, event.method) for event in trace.events
               if event.call >= start and event.method in
               ('create', 'write', 'sync_file', 'sync_dir', 'rename', 'unlink')}
    for call, method in sorted(targets):
        fs = deepcopy(original); io = FaultFS(fs, FaultPlan(call, phase, 'error'))
        db = Engine.open(io)
        with pytest.raises(OSError): db.flush()
        assert db.failed
        calls = io.calls
        for action in (lambda: db.get(b'k'), lambda: db.put(b'new', b'value'),
                       lambda: db.delete(b'k'), db.flush, db.snapshot):
            with pytest.raises(FailedStateError): action()
        assert io.calls == calls
        db.close(); fs.reopen()
        for choice in fs.enumerate_crashes():
            trial = deepcopy(fs); trial.crash(choice)
            assert recover_state(trial, []) == {b'k': b'v'}
        # A failed manifest rename/dir sync can leave a valid dirty namespace.
        assert recover_state(fs, []) == {b'k': b'v'}


def test_cleanup_temp_alias_does_not_overwrite_installed_manifest():
    fs = initialized()
    # Reorder creation + rename of a temporary manifest to produce an alias,
    # as allowed by SimFS. Contents match the current valid initial manifest.
    raw = fs.read('MANIFEST')
    fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', raw); fs.sync_file('MANIFEST.tmp')
    fs.rename('MANIFEST.tmp', 'MANIFEST')
    from lsmkv.simfs import CrashChoice
    fs.crash(CrashChoice(directory_order=(1, 0)))
    assert 'MANIFEST.tmp' in fs.listdir()
    db = Engine.open(fs)
    assert 'MANIFEST.tmp' not in fs.listdir()
    db.put(b'k', b'v'); db.flush(); db.close(); fs.crash()
    assert recover_state(fs, []) == {b'k': b'v'}


def test_referenced_sstable_metadata_mismatch_prevents_all_cleanup():
    for field, change in [('size', 1), ('min_seq', 1), ('max_seq', -1)]:
        fs = SimFS(); db = Engine.open(fs)
        db.put(b'a', b'a'); db.put(b'b', b'b'); db.flush(); db.close(); fs.reopen()
        m = metadata(fs); m['sstables'][0][field] += change
        replace_bytes(fs, 'MANIFEST', encode(m))
        fs.create('sst-999999.sst'); fs.write('sst-999999.sst', b'keep')
        fs.create('orphan.tmp'); fs.write('orphan.tmp', b'keep')
        before = {name: fs.read(name) for name in fs.listdir()}
        with pytest.raises(CorruptionError): Engine.open(fs)
        assert {name: fs.read(name) for name in fs.listdir()} == before


def test_invalid_wal_prevents_orphan_cleanup_too():
    fs = initialized(); fs.create('orphan.tmp'); fs.write('orphan.tmp', b'keep')
    replace_bytes(fs, WAL, HEADER + b'\0' * 12)
    before = {name: fs.read(name) for name in fs.listdir()}
    with pytest.raises(CorruptionError): Engine.open(fs)
    assert {name: fs.read(name) for name in fs.listdir()} == before


def test_existing_file_collision_is_rejected_without_overwrite():
    fs = SimFS(); db = Engine.open(fs); db.put(b'k', b'v')
    # Simulate an unexpected file appearing after allocator initialization.
    fs.create('wal-000002.log'); fs.write('wal-000002.log', b'keep')
    with pytest.raises(FileExistsError): db.flush()
    assert fs.read('wal-000002.log') == b'keep' and db.failed
    db.close()


def test_wal_replay_and_active_selection_use_numeric_file_order():
    fs = initialized()
    for name, seq in [('wal-999999.log', 1), ('wal-1000000.log', 2)]:
        fs.create(name); fs.write(name, HEADER + encode_record(Record(seq, 1, b'k', str(seq).encode())))
        fs.sync_file(name)
    fs.sync_dir()
    db = Engine.open(fs)
    assert db.active_wal == 'wal-1000000.log'
    assert db.next_seq == 3 and db.get(b'k') == b'2'
    db.flush()
    assert db.active_wal == 'wal-1000001.log'
    assert metadata(fs)['sstables'][0]['name'] == 'sst-1000002.sst'
    db.close(); fs.crash()
    assert recover_state(fs, []) == {b'k': b'2'}


def test_repeated_overwrites_do_not_accumulate_live_entry_bytes():
    fs = SimFS(); db = Engine.open(fs, memtable_bytes=40, wal_bytes=10000)
    for _ in range(8):
        db.put(b'k', b'v')
        assert db.live_bytes == 19 and metadata(fs)['generation'] == 0
    db.put(b'z', b'v')
    assert db.live_bytes == 38 and metadata(fs)['generation'] == 0
    db.delete(b'missing')  # Tombstones are live encoded entries too.
    assert metadata(fs)['generation'] == 1 and db.live_bytes == 0
    assert db.snapshot() == {b'k': b'v', b'z': b'v'}
    db.close()


def test_partial_flush_writes_at_every_byte_reopen_without_crash():
    from lsmkv.sstable import encode as encode_sst
    original = flush_factory().fs
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); start = trace.calls
    db.flush(); db.close(); trace.fs.reopen()
    writes = [event.call for event in trace.events
              if event.call >= start and event.method == 'write' and event.phase == 'before']
    payloads = [HEADER, encode_sst({b'k': (1, 1, b'v')}), trace.fs.read('MANIFEST')]
    assert len(writes) == 3
    for call, data in zip(writes, payloads):
        for prefix in range(len(data) + 1):
            fs = deepcopy(original); io = FaultFS(fs, FaultPlan(call, 'before', 'error', write_prefix=prefix))
            db = Engine.open(io)
            with pytest.raises(OSError): db.flush()
            assert db.failed
            db.close()
            assert recover_state(fs, []) == {b'k': b'v'}


def test_recovery_at_watermark_without_any_tables_never_replays_covered_keys():
    # A valid zero-table state is representable by the final manifest format.
    # Construct it to test filtering independently of tombstones; no compactor.
    fs = initialized()
    replace_bytes(fs, WAL, HEADER + encode_record(Record(1, 1, b'old', b'covered')))
    m = {'generation': 1, 'flushed_seq': 1, 'next_file_number': 2, 'sstables': []}
    replace_bytes(fs, 'MANIFEST', encode(m))
    db = Engine.open(fs)
    assert db.memtable == {} and db.snapshot() == {} and db.next_seq == 2
    assert WAL in fs.listdir()  # Covered active WAL is retained.
    db.put(b'new', b'valid'); db.flush(); db.close(); fs.crash()
    assert recover_state(fs, []) == {b'new': b'valid'}
