from copy import deepcopy

import pytest

from lsmkv import Engine
from lsmkv.harness import FaultFS, FaultPlan, InjectedCrash, CrashCase, run_crashes
from test_compaction import compact_source
from test_engine import recover_state


def interrupted_compaction(stage, empty):
    original, expected, _ = compact_source(empty=empty, memtable=True)
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); start = trace.calls
    db.compact(); end = trace.calls; db.close()
    events = [event for event in trace.events if start <= event.call < end and event.phase == 'before']
    if stage == 'before_manifest':
        target = [event.call for event in events if event.method == 'rename'][-1]
    else:
        target = next(event.call for event in events if event.method == 'unlink')
    fs = deepcopy(original); io = FaultFS(fs, FaultPlan(target, 'before', 'crash'))
    with pytest.raises(InjectedCrash):
        db = Engine.open(io); db.compact()
    fs.crash()
    return fs, expected


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('stage', ['before_manifest', 'before_input_delete'])
def test_compaction_recovery_crash_then_second_recovery_crash(empty, stage):
    original, expected = interrupted_compaction(stage, empty)
    second_counts = []
    def first_factory():
        fs = deepcopy(original)
        def recover_with_second_crashes(io):
            second_base = deepcopy(io.fs)
            def second_factory():
                return CrashCase(deepcopy(second_base),
                                 lambda second_io: recover_state(second_io, []),
                                 lambda third_io: recover_state(third_io, []),
                                 lambda state, event: state == expected or pytest.fail('second recovery crash changed state'))
            second_counts.append(len(run_crashes(second_factory)))
            return recover_state(io, [])
        return CrashCase(fs, lambda io: recover_state(io, []), recover_with_second_crashes,
                         lambda state, event: state == expected or pytest.fail('first recovery crash changed state'))
    reports = run_crashes(first_factory)
    assert len(second_counts) == len(reports)
    print(f'M3 recovery {stage}, empty={empty}: {len(reports)} first / {sum(second_counts)} second crash cases')


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('stage', ['before_manifest', 'before_input_delete'])
@pytest.mark.parametrize('phase', ['before', 'after'])
def test_compaction_recovery_io_errors_can_be_reopened(empty, stage, phase):
    original, expected = interrupted_compaction(stage, empty)
    trace = FaultFS(deepcopy(original)); recover_state(trace, [])
    targets = {(event.call, event.method) for event in trace.events if event.method in
               ('read', 'write', 'truncate', 'sync_file', 'sync_dir', 'unlink')}
    for call, method in sorted(targets):
        fs = deepcopy(original); io = FaultFS(fs, FaultPlan(call, phase, 'error'))
        with pytest.raises(OSError): recover_state(io, [])
        # A failed recovery aborts; it does not retry a failed barrier or cleanup.
        assert all(event.method == 'unlock' for event in io.events if event.call > call)
        for choice in fs.enumerate_crashes():
            trial = deepcopy(fs); trial.crash(choice)
            assert recover_state(trial, []) == expected
        assert recover_state(fs, []) == expected
