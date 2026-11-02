from copy import deepcopy

import pytest

from lsmkv import Engine
from lsmkv.harness import FaultFS, FaultPlan, InjectedCrash, CrashCase, run_crashes
from test_m2 import flush_factory
from test_engine import recover_state


def interrupted_flush(stage):
    original = flush_factory().fs
    trace = FaultFS(deepcopy(original)); db = Engine.open(trace); db.flush(); db.close()
    if stage == 'before_manifest':
        target = [event.call for event in trace.events if event.method == 'rename' and event.phase == 'before'][1]
    else:
        target = next(event.call for event in trace.events if event.method == 'unlink')
    fs = deepcopy(original); io = FaultFS(fs, FaultPlan(target, 'before', 'crash'))
    with pytest.raises(InjectedCrash):
        db = Engine.open(io); db.flush()
    fs.crash()
    return fs


@pytest.mark.parametrize('stage', ['before_manifest', 'before_retirement'])
def test_recovery_crash_then_second_recovery_crash(stage):
    original = interrupted_flush(stage)
    second_counts = []
    def first_factory():
        fs = deepcopy(original)
        def recover_with_second_crashes(io):
            second_base = deepcopy(io.fs)
            def second_factory():
                return CrashCase(deepcopy(second_base),
                                 lambda second_io: recover_state(second_io, []),
                                 lambda third_io: recover_state(third_io, []),
                                 lambda image, event: image == {b'k': b'v'} or pytest.fail('second recovery lost data'))
            second_counts.append(len(run_crashes(second_factory)))
            return recover_state(io, [])
        return CrashCase(fs, lambda io: recover_state(io, []), recover_with_second_crashes,
                         lambda image, event: image == {b'k': b'v'} or pytest.fail('first recovery lost data'))
    reports = run_crashes(first_factory)
    assert len(second_counts) == len(reports)
    print(f'M2 {stage}: {len(reports)} first recovery crashes; {sum(second_counts)} second recovery crashes')
