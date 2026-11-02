from copy import deepcopy
import tracemalloc

import pytest

from lsmkv.simfs import SimFS, CrashChoice
from lsmkv.model import History, Operation
from lsmkv.harness import FaultFS, FaultPlan, InjectedCrash, CrashCase, run_crashes


def test_compact_log_preserves_every_mixed_microstep():
    fs = SimFS(); fs.create('a'); fs.write('a', b'base'); fs.sync_file('a'); fs.sync_dir()
    fs.write('a', b'XY', 1); fs.truncate('a', 2); fs.write('a', b'ZQ', 4)
    expected = [b'base', b'bXse', b'bXYe', b'bX', b'bX\0\0Z', b'bX\0\0ZQ']
    choices = list(fs.enumerate_crashes())
    assert len(choices) == len(expected)
    for choice, contents in zip(choices, expected):
        trial = deepcopy(fs); trial.crash(choice)
        assert trial.read('a') == contents


def test_large_write_has_linear_memory():
    tracemalloc.start()
    try:
        fs = SimFS(); fs.create('a'); fs.sync_dir()
        data = b'x' * (2 * 1024 * 1024)
        fs.write('a', data)
        assert fs.read('a') == data
        fs.crash(CrashChoice({'a': len(data) // 2}))
        assert fs.read('a') == data[:len(data) // 2]
        _, peak = tracemalloc.get_traced_memory()
        assert peak < 24 * 1024 * 1024
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize('keep', [False, True])
def test_directory_creation_parent_barrier_and_lifecycle(keep):
    fs = SimFS(directory_exists=False)
    assert not fs.directory_exists()
    with pytest.raises(FileNotFoundError): fs.listdir()
    fs.ensure_directory(); fs.create('a'); fs.write('a', b'x')
    fs.sync_file('a'); fs.sync_dir()
    fs.crash(CrashChoice(directory_survives=keep))
    assert fs.directory_exists() is keep
    fs.ensure_directory(); fs.sync_parent(); fs.sync_dir()
    for choice in fs.enumerate_crashes():
        trial = deepcopy(fs); trial.crash(choice)
        assert trial.directory_exists()
    token = fs.lock('LOCK'); fs.close(); fs.close()
    with pytest.raises(ValueError): fs.listdir()
    fs.reopen(); fs.lock('LOCK')
    with pytest.raises(ValueError): fs.unlock(token)


def test_close_does_not_sync_and_reopen_does_not_recover():
    fs = SimFS(); fs.create('a'); fs.write('a', b'x')
    fs.close(); fs.reopen()
    assert fs.read('a') == b'x'
    fs.crash()
    assert fs.listdir() == []


def test_parent_sync_is_independent_of_directory_and_contents():
    fs = SimFS(directory_exists=False)
    fs.ensure_directory(); fs.create('a'); fs.write('a', b'x'); fs.sync_parent()
    fs.crash()
    assert fs.directory_exists()
    assert fs.listdir() == []


def test_both_barriers_satisfy_history_for_all_outcomes():
    fs = SimFS(); fs.create('ack'); fs.write('ack', b'value')
    fs.sync_file('ack'); fs.sync_dir()
    # Pending unrelated work makes this a nontrivial crash product.
    fs.create('tmp'); fs.write('tmp', b'xy'); fs.rename('tmp', 'other')
    h = History(); h.begin(Operation('put', b'k', b'value')); h.acknowledge()
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 15
    for choice in choices:
        trial = deepcopy(fs); trial.crash(choice)
        h.check({b'k': trial.read('ack')})


def test_faults_before_after_and_partial_write():
    fs = SimFS(); fs.create('a'); fs.sync_dir()
    io = FaultFS(fs, FaultPlan(0, 'before', 'error', write_prefix=2))
    with pytest.raises(OSError): io.write('a', b'1234')
    assert fs.read('a') == b'12'
    fs.crash(CrashChoice({'a': 1})); assert fs.read('a') == b'1'
    io = FaultFS(fs, FaultPlan(0, 'before', 'error'))
    with pytest.raises(OSError): io.sync_file('a')
    io = FaultFS(fs, FaultPlan(0, 'after', 'error'))
    fs.write('a', b'2')
    with pytest.raises(OSError): io.sync_file('a')
    fs.crash(); assert fs.read('a') == b'12'
    io = FaultFS(fs, FaultPlan(0, 'after', 'crash'))
    with pytest.raises(InjectedCrash): io.truncate('a', 0)
    assert fs.read('a') == b''  # wrapper stops execution; runner selects crash image


def make_case():
    fs = SimFS(); fs.create('a'); fs.sync_file('a'); fs.sync_dir()
    h = History()
    def workload(io):
        h.begin(Operation('put', b'k', b'x'))
        io.write('a', b'x'); io.sync_file('a')
        h.acknowledge()
        io.listdir()
    def recover(io):
        data = io.read('a')
        return {b'k': data} if data else {}
    return CrashCase(fs, workload, recover, lambda image, event: h.check(image))


def test_runner_visits_each_boundary_and_replays_choices():
    reports = run_crashes(make_case)
    assert {(r.event.call, r.event.phase) for r in reports} == {
        (call, phase) for call in range(3) for phase in ('before', 'after')} | {(3, 'complete')}
    assert len(reports) == 9  # two outcomes after write/before sync, plus completion
    assert reports == run_crashes(make_case)
    sampled = run_crashes(make_case, seeds=(7, 11))
    assert len(sampled) == 14
    assert sampled == run_crashes(make_case, seeds=(7, 11))


def test_runner_detects_missing_sync():
    def factory():
        fs = SimFS(); fs.create('a'); fs.sync_file('a'); fs.sync_dir()
        h = History()
        def bad(io):
            h.begin(Operation('put', b'k', b'x'))
            io.write('a', b'x')
            h.acknowledge()  # Deliberately violates the file-sync requirement.
            io.listdir()
        def recover(io):
            data = io.read('a')
            return {b'k': data} if data else {}
        return CrashCase(fs, bad, recover, lambda image, event: h.check(image))
    with pytest.raises(AssertionError, match='allowed states'):
        run_crashes(factory)


def test_pending_write_memory_is_linear_small_regression():
    tracemalloc.start()
    try:
        fs = SimFS(); fs.create('a')
        fs.write('a', b'x' * 16384)
        _, peak = tracemalloc.get_traced_memory()
        assert peak < 16 * 16384
    finally:
        tracemalloc.stop()


def test_error_plan_is_one_shot_and_preserves_completed_barriers():
    fs = SimFS(); fs.create('a'); fs.write('a', b'x')
    io = FaultFS(fs, FaultPlan(0, 'after', 'error'))
    with pytest.raises(OSError) as error: io.sync_dir()
    import errno
    assert error.value.errno == errno.EIO
    io.sync_file('a')  # A generic injector permits calls; a future engine must fail closed.
    fs.crash()
    assert fs.read('a') == b'x'
    fs.write('a', b'y')
    io = FaultFS(fs, FaultPlan(0, 'before', 'error'))
    with pytest.raises(OSError): io.sync_file('a')
    fs.crash()
    assert fs.read('a') == b'x'


def test_error_injection_wraps_reads_and_namespace_calls():
    fs = SimFS(); fs.create('a'); fs.write('a', b'x'); fs.sync_file('a'); fs.sync_dir()
    operations = [lambda io: io.read('a'), lambda io: io.rename('a', 'b'),
                  lambda io: io.unlink('a'), lambda io: io.truncate('a', 0)]
    for action in operations:
        trial = deepcopy(fs)
        io = FaultFS(trial, FaultPlan(0, 'before', 'error'))
        with pytest.raises(OSError): action(io)
        assert trial.listdir() == ['a']
        assert trial.read('a') == b'x'


def test_runner_can_crash_recovery_and_check_second_recovery():
    def factory():
        fs = SimFS(); fs.create('a'); fs.write('a', b'good-tail'); fs.sync_file('a'); fs.sync_dir()
        fs.crash()
        def recover(io):
            io.truncate('a', 4); io.sync_file('a')
            return io.read('a')
        def workload(io):
            recover(io)
        def check(image, event):
            assert image == b'good'
        return CrashCase(fs, workload, recover, check)
    reports = run_crashes(factory)
    assert len(reports) == 9
    assert {r.event.method for r in reports} == {'truncate', 'sync_file', 'read', 'workload'}


def test_runner_covers_provisioning_and_close_calls():
    def factory():
        fs = SimFS(directory_exists=False)
        def workload(io):
            io.directory_exists(); io.ensure_directory(); io.sync_parent()
            io.sync_dir(); io.close(); io.reopen()
        def recover(io):
            io.ensure_directory(); io.sync_parent()
            return io.directory_exists()
        return CrashCase(fs, workload, recover, lambda image, event: image is True or pytest.fail('missing root'))
    reports = run_crashes(factory)
    assert {(r.event.call, r.event.phase) for r in reports} == {
        (call, phase) for call in range(6) for phase in ('before', 'after')} | {(6, 'complete')}
    assert len(reports) == 15  # root outcomes after mkdir/before parent sync, plus completion


def test_runner_checks_acknowledgment_after_final_fs_call():
    def factory():
        fs = SimFS(); fs.create('a'); fs.sync_file('a'); fs.sync_dir()
        h = History()
        def workload(io):
            h.begin(Operation('put', b'k', b'x'))
            io.write('a', b'x')
            h.acknowledge()  # No later FS call exists at which to inject a crash.
        def recover(io):
            data = io.read('a')
            return {b'k': data} if data else {}
        return CrashCase(fs, workload, recover, lambda image, event: h.check(image))
    with pytest.raises(AssertionError): run_crashes(factory)


def test_compact_log_matches_tiny_snapshot_reference():
    import random
    for seed in range(20):
        rng = random.Random(seed)
        fs = SimFS(); fs.create('a'); fs.write('a', b'ab'); fs.sync_file('a'); fs.sync_dir()
        images = [b'ab']
        live = b'ab'
        for _ in range(6):
            if rng.getrandbits(1):
                size = rng.randrange(6)
                fs.truncate('a', size)
                live = live[:size].ljust(size, b'\0')
                images.append(live)
            else:
                offset = rng.randrange(6)
                data = rng.randbytes(rng.randrange(4))
                fs.write('a', data, offset)
                for index, byte in enumerate(data, offset):
                    live += b'\0' * max(0, index - len(live))
                    live = live[:index] + bytes([byte]) + live[index + 1:]
                    images.append(live)
            assert fs.read('a') == live
        choices = list(fs.enumerate_crashes())
        assert len(choices) == len(images)
        for choice, expected in zip(choices, images):
            trial = deepcopy(fs); trial.crash(choice)
            assert trial.read('a') == expected


def test_crash_does_not_allow_finally_to_complete_a_sync():
    fs = SimFS(); fs.create('a'); fs.sync_dir()
    io = FaultFS(fs, FaultPlan(0, 'after', 'crash'))
    with pytest.raises(InjectedCrash):
        try:
            io.write('a', b'x')
        finally:
            io.sync_file('a')
    fs.crash()
    assert fs.read('a') == b''
