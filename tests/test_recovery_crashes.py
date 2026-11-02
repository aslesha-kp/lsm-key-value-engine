from copy import deepcopy

import pytest

from lsmkv.engine import Engine
from lsmkv.errors import CorruptionError, MissingManifestError
from lsmkv.harness import CrashCase, FaultFS, FaultPlan, InjectedCrash, run_crashes
from lsmkv.model import History, Operation
from lsmkv.simfs import SimFS
from lsmkv.wal import HEADER, Record, encode_record
from test_engine import initialized, replace_bytes, recover_state, WAL


def test_initialization_and_first_ack_at_every_call_and_crash_outcome():
    def factory():
        fs = SimFS(directory_exists=False); h = History()
        def workload(io):
            db = Engine.open(io)
            h.begin(Operation('put', b'k', b'v'))
            db.put(b'k', b'v'); h.acknowledge(); db.close()
        return CrashCase(fs, workload, lambda io: recover_state(io, [b'k']),
                         lambda image, event: h.check(image))
    reports = run_crashes(factory)
    assert any(r.event.method == 'sync_parent' for r in reports)
    assert any(r.choice.directory_survives is False for r in reports)
    print(f'initialization + first write: {len(reports)} exhaustive crash cases')


def recovery_image(kind):
    if kind == 'tail':
        fs = initialized()
        first = encode_record(Record(1, 1, b'k', b'good'))
        tail = encode_record(Record(2, 1, b'z', b'unfinished'))[:-1]
        replace_bytes(fs, WAL, HEADER + first + tail)
        return fs, {b'k': b'good'}
    if kind == 'short_header':
        fs = initialized(); replace_bytes(fs, WAL, HEADER[:5])
        return fs, {}
    fs = SimFS(); fs.create(WAL); fs.write(WAL, HEADER)
    fs.sync_file(WAL); fs.sync_dir()
    fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', b'incomplete')
    fs.sync_file('MANIFEST.tmp'); fs.sync_dir()
    return fs, {}


@pytest.mark.parametrize('kind', ['tail', 'short_header', 'initial_manifest'])
def test_crash_during_recovery_then_second_recovery_crash(kind):
    original, expected = recovery_image(kind)
    keys = [b'k', b'z']
    second_counts = []
    def first_factory():
        fs = deepcopy(original)
        def second_recovery_with_crashes(io):
            # The first crash has happened. The second runner starts from that
            # exact recovered FS image and crashes each call in the next open.
            second_base = deepcopy(io.fs)
            def second_factory():
                second_fs = deepcopy(second_base)
                def workload(second_io):
                    assert recover_state(second_io, keys) == expected
                return CrashCase(second_fs, workload,
                                 lambda third_io: recover_state(third_io, keys),
                                 lambda state, event: state == expected or pytest.fail('second crash lost data'))
            second_counts.append(len(run_crashes(second_factory)))
            return recover_state(io, keys)
        return CrashCase(fs, lambda io: recover_state(io, keys),
                         second_recovery_with_crashes,
                         lambda state, event: state == expected or pytest.fail('first crash lost data'))
    reports = run_crashes(first_factory)
    assert len(second_counts) == len(reports)
    print(f'{kind}: {len(reports)} first crashes; {sum(second_counts)} second crashes, exhaustive per boundary')


@pytest.mark.parametrize('phase', ['before', 'after'])
def test_every_initialization_write_and_sync_error_recovers(phase):
    original = SimFS(directory_exists=False)
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); db.close()
    targets = {(e.call, e.method) for e in trace.events
               if e.method in ('create', 'write', 'truncate', 'sync_file', 'sync_dir', 'sync_parent', 'rename')}
    for call, method in sorted(targets):
        fs = deepcopy(original)
        io = FaultFS(fs, FaultPlan(call, phase, 'error'))
        with pytest.raises(OSError): Engine.open(io)
        # Enumerate the pending failure image, not just drop-all on crash.
        for choice in fs.enumerate_crashes():
            trial = deepcopy(fs); trial.crash(choice)
            db = Engine.open(trial); db.put(b'k', b'v'); db.close(); trial.crash()
            assert recover_state(trial, [b'k']) == {b'k': b'v'}
        # Also reopen with dirty cache/name state left by the failed open.
        db = Engine.open(fs); db.put(b'k', b'v'); db.close(); fs.crash()
        assert recover_state(fs, [b'k']) == {b'k': b'v'}


def test_missing_manifest_strictness_and_approved_tmp_exception():
    for initial in (b'', HEADER[:4], HEADER):
        fs = SimFS(); fs.create(WAL); fs.write(WAL, initial)
        fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', b'garbage')
        fs.sync_file(WAL); fs.sync_file('MANIFEST.tmp'); fs.sync_dir()
        db = Engine.open(fs); db.put(b'k', b'v'); db.close()
    for contents in (b'BADMAGIC' + HEADER[8:], HEADER + b'x', HEADER + encode_record(Record(1, 1, b'k', b'v'))):
        fs = SimFS(); fs.create(WAL); fs.write(WAL, contents)
        fs.create('MANIFEST.tmp'); fs.write('MANIFEST.tmp', b'keep')
        before = {name: fs.read(name) for name in fs.listdir()}
        with pytest.raises(MissingManifestError): Engine.open(fs)
        assert all(fs.read(name) == data for name, data in before.items())
        assert set(before) <= set(fs.listdir())


@pytest.mark.parametrize('kind', ['tail', 'short_header', 'initial_manifest'])
@pytest.mark.parametrize('phase', ['before', 'after'])
def test_recovery_write_and_sync_errors_abort_without_retry(kind, phase):
    original, expected = recovery_image(kind)
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); db.close()
    targets = {(event.call, event.method) for event in trace.events
               if event.method in ('write', 'truncate', 'sync_file', 'sync_dir', 'sync_parent')}
    for call, method in sorted(targets):
        fs = deepcopy(original); io = FaultFS(fs, FaultPlan(call, phase, 'error'))
        with pytest.raises(OSError): Engine.open(io)
        # No retry of the failing write/barrier occurs; only unlock may follow.
        assert all(event.method == 'unlock' for event in io.events if event.call > call)
        for choice in fs.enumerate_crashes():
            trial = deepcopy(fs); trial.crash(choice)
            assert recover_state(trial, [b'k', b'z']) == expected


def test_partial_initialization_header_and_manifest_writes_at_every_byte():
    from lsmkv.manifest import encode_initial
    source = SimFS(directory_exists=False)
    trace = FaultFS(deepcopy(source)); db = Engine.open(trace); db.close()
    writes = [event.call for event in trace.events if event.method == 'write' and event.phase == 'before']
    assert len(writes) == 2
    for call, data in zip(writes, (HEADER, encode_initial())):
        for count in range(len(data) + 1):
            fs = deepcopy(source)
            io = FaultFS(fs, FaultPlan(call, 'before', 'error', write_prefix=count))
            with pytest.raises(OSError): Engine.open(io)
            # Reopen with partial bytes still visible, then verify a new ack.
            db = Engine.open(fs); db.put(b'k', b'v'); db.close(); fs.crash()
            assert recover_state(fs, [b'k']) == {b'k': b'v'}
