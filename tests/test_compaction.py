from copy import deepcopy

import pytest

from lsmkv import Engine, FailedStateError
from lsmkv.harness import FaultFS, FaultPlan, CrashCase, run_crashes
from lsmkv.model import History, Operation, generate_operations
from lsmkv.simfs import SimFS
from lsmkv.sstable import decode as decode_table, encode as encode_table
from test_engine import recover_state, WAL
from test_m2 import metadata


def compact_source(*, empty=False, memtable=False):
    fs = SimFS(); db = Engine.open(fs)
    db.put(b'gone', b'old')
    if not empty: db.put(b'live', b'v')
    old_wal = fs.read(db.active_wal)
    db.flush()
    db.delete(b'gone'); db.flush()
    if memtable:
        db.put(b'new', b'mem')
        if not empty: db.delete(b'live')
    expected = db.snapshot()
    db.close(); fs.reopen()
    return fs, expected, old_wal


def compact_factory(*, empty=False, memtable=False):
    def factory():
        fs, expected, _ = compact_source(empty=empty, memtable=memtable)
        h = History()
        for key, value in expected.items():
            h.begin(Operation('put', key, value)); h.acknowledge()
        def workload(io):
            db = Engine.open(io); db.compact(); h.check(db.snapshot()); db.close()
        return CrashCase(fs, workload, lambda io: recover_state(io, []),
                         lambda state, event: h.check(state))
    return factory


def test_full_merge_selects_newest_and_preserves_newer_memtable_versions():
    fs = SimFS(); db = Engine.open(fs)
    for key in (b'a', b'b', b'c'): db.put(key, b'old')
    db.flush()
    db.put(b'a', b'table-new'); db.delete(b'c'); db.flush()
    db.put(b'a', b'mem-new'); db.delete(b'b'); db.put(b'c', b'reborn')
    before = metadata(fs); inputs = {item['name'] for item in before['sstables']}
    mem, live, active, seq = db.memtable, db.live_bytes, db.active_wal, db.next_seq
    wal = fs.read(active)
    db.compact()
    after = metadata(fs)
    assert len(after['sstables']) == 1
    assert after['flushed_seq'] == before['flushed_seq'] == 5
    assert after['generation'] == before['generation'] + 1
    assert after['next_file_number'] == before['next_file_number'] + 1
    output = after['sstables'][0]
    assert output['name'] not in inputs
    assert decode_table(fs.read(output['name'])).entries == {
        b'a': (4, 1, b'table-new'), b'b': (2, 1, b'old')}
    assert output['max_seq'] == 4 < after['flushed_seq']
    assert not inputs.intersection(fs.listdir())
    assert (db.memtable, db.live_bytes, db.active_wal, db.next_seq) == (mem, live, active, seq)
    assert fs.read(active) == wal
    assert db.snapshot() == {b'a': b'mem-new', b'c': b'reborn'}
    for key, expected in [(b'a', b'mem-new'), (b'b', None), (b'c', b'reborn')]:
        assert db.get(key) == expected
    db.close(); fs.crash(); db = Engine.open(fs)
    assert db.snapshot() == {b'a': b'mem-new', b'c': b'reborn'} and db.next_seq == seq
    db.flush(); db.compact()
    assert db.snapshot() == {b'a': b'mem-new', b'c': b'reborn'}
    assert all(entry[1] == 1 for entry in decode_table(fs.read(metadata(fs)['sstables'][0]['name'])).entries.values())
    db.close()


def test_all_deleted_output_preserves_watermark_counter_and_next_sequence():
    fs, _, _ = compact_source(empty=True)
    io = FaultFS(fs); db = Engine.open(io)
    before = metadata(fs); active, sequence = db.active_wal, db.next_seq
    start = io.calls
    db.compact()
    after = metadata(fs)
    assert after == {**before, 'generation': before['generation'] + 1, 'sstables': []}
    assert not any(name.startswith('sst-') for name in fs.listdir())
    assert (db.active_wal, db.next_seq) == (active, sequence)
    compact_events = [event for event in io.events if event.call >= start and event.phase == 'before']
    assert sum(event.method == 'create' for event in compact_events) == 1  # manifest only
    assert sum(event.method == 'write' for event in compact_events) == 1
    # With zero tables, repeated compaction neither installs a manifest nor allocates.
    calls = io.calls; db.compact(); assert io.calls == calls
    db.close(); fs.crash(); db = Engine.open(fs)
    assert db.snapshot() == {} and db.next_seq == 3
    db.put(b'new', b'value'); assert db.next_seq == 4
    db.flush(); db.compact(); db.close(); fs.crash()
    assert recover_state(fs, []) == {b'new': b'value'}


def test_empty_output_does_not_flush_or_clear_newer_memtable():
    fs, expected, _ = compact_source(empty=True, memtable=True)
    db = Engine.open(fs); before = metadata(fs); mem = db.memtable
    db.compact()
    assert metadata(fs)['sstables'] == []
    assert metadata(fs)['flushed_seq'] == before['flushed_seq']
    assert db.memtable == mem and db.snapshot() == expected == {b'new': b'mem'}
    db.close(); fs.crash(); assert recover_state(fs, []) == expected


def test_repeated_nonempty_compaction_uses_fresh_ids_and_keeps_watermark():
    fs, expected, _ = compact_source()
    db = Engine.open(fs)
    seen = {item['name'] for item in metadata(fs)['sstables']}
    watermark = metadata(fs)['flushed_seq']
    for _ in range(4):
        db.compact()
        m = metadata(fs); name = m['sstables'][0]['name']
        assert name not in seen and m['flushed_seq'] == watermark
        assert not seen.intersection(fs.listdir())
        seen.add(name)
        assert db.snapshot() == expected
    db.put(b'live', b'newer'); db.flush(); db.compact()
    assert db.snapshot() == {b'live': b'newer'}
    db.close(); fs.crash(); assert recover_state(fs, []) == {b'live': b'newer'}


@pytest.mark.parametrize('empty', [False, True])
def test_compaction_watermark_blocks_retained_wal_resurrection(empty):
    fs, expected, old_wal = compact_source(empty=empty)
    db = Engine.open(fs); db.compact()
    m = metadata(fs)
    assert not any(entry[1] == 2 for table in m['sstables']
                   for entry in decode_table(fs.read(table['name'])).entries.values())
    db.close(); fs.reopen()
    # Construct a redundant covered-WAL image. The installed watermark, rather
    # than a now-discarded tombstone, must prevent the old put from resurfacing.
    fs.create(WAL); fs.write(WAL, old_wal); fs.sync_file(WAL); fs.sync_dir()
    db = Engine.open(fs)
    h = History()
    for key, value in expected.items():
        h.begin(Operation('put', key, value)); h.acknowledge()
    h.check(db.snapshot())
    assert db.get(b'gone') is None and db.memtable == {}
    assert db.next_seq == m['flushed_seq'] + 1
    assert WAL not in fs.listdir()
    db.close()


@pytest.mark.parametrize('empty', [False, True])
def test_compaction_crashes_at_every_call_and_modeled_outcome(empty):
    reports = run_crashes(compact_factory(empty=empty, memtable=True))
    print(f'M3 compact empty={empty}: {len(reports)} exhaustive boundary/persistence cases')
    assert any(report.event.method == 'unlink' for report in reports)


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('phase', ['before', 'after'])
def test_compaction_io_errors_fail_closed_and_recover(empty, phase):
    original, expected, _ = compact_source(empty=empty, memtable=True)
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); start = trace.calls
    db.compact(); end = trace.calls; db.close()
    targets = {(event.call, event.method) for event in trace.events if start <= event.call < end}
    for call, method in sorted(targets):
        fs = deepcopy(original); io = FaultFS(fs, FaultPlan(call, phase, 'error'))
        db = Engine.open(io)
        with pytest.raises(OSError): db.compact()
        assert db.failed
        assert all(event.call <= call for event in io.events)
        calls = io.calls
        for action in (lambda: db.get(b'live'), lambda: db.put(b'key', b'value'),
                       lambda: db.delete(b'live'), db.flush, db.compact, db.snapshot):
            with pytest.raises(FailedStateError): action()
        assert io.calls == calls
        db.close(); fs.reopen()
        for choice in fs.enumerate_crashes():
            trial = deepcopy(fs); trial.crash(choice)
            assert recover_state(trial, []) == expected
        assert recover_state(fs, []) == expected


@pytest.mark.parametrize('empty', [False, True])
def test_partial_compaction_writes_at_every_byte(empty):
    original, expected, _ = compact_source(empty=empty, memtable=True)
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); start = trace.calls
    db.compact(); end = trace.calls; db.close(); trace.fs.reopen()
    writes = [event.call for event in trace.events if start <= event.call < end
              and event.method == 'write' and event.phase == 'before']
    m = metadata(trace.fs)
    payloads = ([trace.fs.read(m['sstables'][0]['name'])] if m['sstables'] else []) + [trace.fs.read('MANIFEST')]
    assert len(writes) == len(payloads)
    for call, data in zip(writes, payloads):
        for prefix in range(len(data) + 1):
            fs = deepcopy(original)
            io = FaultFS(fs, FaultPlan(call, 'before', 'error', write_prefix=prefix))
            db = Engine.open(io)
            with pytest.raises(OSError): db.compact()
            assert db.failed
            db.close()
            assert recover_state(fs, []) == expected


def test_compact_without_tables_does_not_flush_memtable_or_touch_storage():
    fs = SimFS(); io = FaultFS(fs); db = Engine.open(io)
    db.put(b'key', b'value'); mem, calls = db.memtable, io.calls
    db.compact()
    assert db.memtable == mem and io.calls == calls
    db.close()


@pytest.mark.parametrize('seed', [5, 19])
def test_seeded_workloads_with_flush_and_repeated_full_compaction(seed):
    ops = list(generate_operations(seed, 25, key_count=4))
    def factory():
        fs = SimFS(); h = History()
        def workload(io):
            db = Engine.open(io, memtable_bytes=50, wal_bytes=110)
            for index, op in enumerate(ops):
                h.begin(op)
                if op.kind == 'put': db.put(op.key, op.value)
                elif op.kind == 'delete': db.delete(op.key)
                else: assert db.get(op.key) == h.state.get(op.key)
                h.acknowledge()
                if index % 4 == 0: db.compact()
                h.check(db.snapshot())
            db.compact(); db.close()
        return CrashCase(fs, workload, lambda io: recover_state(io, []),
                         lambda state, event: h.check(state))
    reports = run_crashes(factory, seeds=(seed, seed + 1000))
    assert {report.seed for report in reports} == {seed, seed + 1000}


def test_empty_compaction_preserves_manifest_counter_and_observed_allocator_floor():
    fs, _, _ = compact_source(empty=True)
    fs.create('sst-000100.sst.tmp'); fs.write('sst-000100.sst.tmp', b'orphan')
    fs.sync_dir()
    db = Engine.open(fs); counter = metadata(fs)['next_file_number']
    db.compact()
    assert metadata(fs)['next_file_number'] == counter
    db.put(b'new', b'value'); db.flush()
    assert db.active_wal == 'wal-000101.log'
    assert metadata(fs)['sstables'][0]['name'] == 'sst-000102.sst'
    db.close()


@pytest.mark.parametrize('suffix', ['', '.tmp'])
def test_compaction_refuses_existing_output_name_without_overwriting(suffix):
    fs, expected, _ = compact_source()
    db = Engine.open(fs); before = metadata(fs)
    name = f"sst-{before['next_file_number']:06d}.sst" + suffix
    fs.create(name); fs.write(name, b'keep')
    with pytest.raises(FileExistsError): db.compact()
    assert db.failed and fs.read(name) == b'keep' and metadata(fs) == before
    assert all(table['name'] in fs.listdir() for table in before['sstables'])
    db.close(); assert recover_state(fs, []) == expected
