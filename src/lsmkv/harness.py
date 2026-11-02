"""Serial FS call boundary crash/error injection, independent of any engine."""
from copy import deepcopy
from dataclasses import dataclass
import errno
from typing import Callable

from .simfs import SimFS, CrashChoice


# Only storage calls are intercepted; simulator controls are never intercepted.
FS_CALLS = frozenset(('directory_exists', 'ensure_directory', 'sync_parent',
    'close', 'reopen', 'create', 'read', 'write', 'truncate', 'listdir',
    'sync_file', 'sync_dir', 'rename', 'unlink', 'lock', 'unlock'))


@dataclass(frozen=True)
class Event:
    call: int
    method: str
    phase: str


@dataclass(frozen=True)
class FaultPlan:
    call: int
    phase: str
    kind: str
    write_prefix: int | None = None

    def __post_init__(self):
        if self.call < 0 or self.phase not in ('before', 'after') or self.kind not in ('error', 'crash'):
            raise ValueError('invalid fault plan')
        if self.write_prefix is not None and (self.write_prefix < 0 or self.phase != 'before' or self.kind != 'error'):
            raise ValueError('partial writes require a before-call error')


class InjectedCrash(BaseException):
    """Control-flow stop, deliberately outside ordinary Exception handlers.

    The harness subsequently calls SimFS.crash with a recorded persistence choice.
    """
    def __init__(self, event):
        self.event = event


class FaultFS:
    def __init__(self, fs, plan=None):
        self.fs = fs
        self.plan = plan
        self.events = []
        self.calls = 0
        self._crashed = None

    @property
    def sync_primitive(self):
        return self.fs.sync_primitive

    def _boundary(self, event, args, kwargs):
        self.events.append(event)
        plan = self.plan
        if plan is None or (event.call, event.phase) != (plan.call, plan.phase):
            return
        # One-shot: caught errors must not repeatedly fire or be retried by us.
        self.plan = None
        if plan.kind == 'crash':
            self._crashed = InjectedCrash(event)
            raise self._crashed
        if plan.write_prefix is not None:
            if event.method != 'write':
                raise ValueError('write_prefix targeted a non-write call')
            # Bind arguments to support both positional and keyword calls.
            import inspect
            bound = inspect.signature(self.fs.write).bind(*args, **kwargs)
            bound.apply_defaults()
            data = bound.arguments['data']
            if plan.write_prefix > len(data):
                raise ValueError('write_prefix exceeds write length')
            bound.arguments['data'] = data[:plan.write_prefix]
            self.fs.write(*bound.args, **bound.kwargs)
        raise OSError(errno.EIO, f'injected {event.phase} {event.method} at call {event.call}')

    def __getattr__(self, name):
        if name not in FS_CALLS:
            raise AttributeError(name)
        def invoke(*args, **kwargs):
            if self._crashed is not None:
                raise self._crashed
            call = self.calls
            self.calls += 1
            self._boundary(Event(call, name, 'before'), args, kwargs)
            result = getattr(self.fs, name)(*args, **kwargs)
            self._boundary(Event(call, name, 'after'), args, kwargs)
            return result
        return invoke


@dataclass
class CrashCase:
    fs: SimFS
    workload: Callable
    recover: Callable
    check: Callable


@dataclass(frozen=True)
class CrashReport:
    event: Event
    choice: CrashChoice
    seed: int | None


def run_crashes(factory, *, seeds=None):
    """Replay a fresh deterministic case at each before/after call boundary.

    Default: enumerate all modeled persistence choices at each reached boundary,
    including workload completion after the final acknowledgment.
    seeds: sample one choice per seed per boundary, recorded in returned reports.
    recover receives a fresh, reopened image via FaultFS; check(image, event)
    should assert correctness using the workload's operation history. Exceptions
    propagate. Use recovery itself as workload in another case to test crashes
    during recovery. Workloads must be deterministic and terminate normally in
    the no-fault discovery run. This is not exhaustive over arbitrary workloads.
    """
    if seeds is not None:
        seeds = tuple(seeds)
        if not seeds:
            raise ValueError('provide at least one sampling seed')
    baseline = factory()
    discovery = FaultFS(baseline.fs)
    baseline.workload(discovery)
    reports = []
    boundaries = discovery.events + [Event(discovery.calls, 'workload', 'complete')]
    for event in boundaries:
        case = factory()
        if event.phase == 'complete':
            io = FaultFS(case.fs)
            case.workload(io)
            if io.events != discovery.events:
                raise AssertionError('workload call trace changed during replay')
        else:
            io = FaultFS(case.fs, FaultPlan(event.call, event.phase, 'crash'))
            try:
                case.workload(io)
            except InjectedCrash as crash:
                if crash.event != event or io.events != discovery.events[:len(io.events)]:
                    raise AssertionError('workload call trace changed during replay')
            else:
                raise AssertionError('crash boundary was not reached')
        choices = ((choice, None) for choice in case.fs.enumerate_crashes()) if seeds is None else (
            (case.fs.sample_crash(seed), seed) for seed in seeds)
        for choice, seed in choices:
            trial = deepcopy(case.fs)
            trial.crash(choice)
            trial.reopen()
            image = case.recover(FaultFS(trial))
            case.check(image, event)
            reports.append(CrashReport(event, choice, seed))
    return reports
