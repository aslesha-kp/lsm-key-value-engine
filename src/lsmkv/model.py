"""Whole-state oracle for serial acknowledged operations and one in flight."""
from dataclasses import dataclass
import random


@dataclass(frozen=True)
class Operation:
    kind: str
    key: bytes
    value: bytes | None = None

    def __post_init__(self):
        if self.kind not in ('put', 'get', 'delete'):
            raise ValueError('unknown operation')
        if not isinstance(self.key, bytes) or (self.kind == 'put' and not isinstance(self.value, bytes)):
            raise TypeError('keys and put values must be bytes')
        if self.kind != 'put' and self.value is not None:
            raise ValueError('only puts have values')


def apply(state, op):
    if op.kind == 'put':
        state[op.key] = op.value
    elif op.kind == 'delete':
        state.pop(op.key, None)
    return state.get(op.key)


class History:
    def __init__(self):
        self.state = {}
        self.acknowledged = []
        self.inflight = None

    def begin(self, op):
        if self.inflight is not None:
            raise RuntimeError('only one operation may be in flight')
        self.inflight = op

    def acknowledge(self):
        if self.inflight is None:
            raise RuntimeError('no operation in flight')
        result = apply(self.state, self.inflight)
        self.acknowledged.append(self.inflight)
        self.inflight = None
        return result

    def check(self, recovered):
        candidates = [self.state]
        if self.inflight is not None:
            candidate = self.state.copy()
            apply(candidate, self.inflight)
            candidates.append(candidate)
        if recovered not in candidates:
            raise AssertionError(f'recovered {recovered!r}; allowed states {candidates!r}')

    def recover(self, recovered):
        self.check(recovered)
        self.state = dict(recovered)
        self.inflight = None


def generate_operations(seed, count, key_count=8):
    if count < 0 or key_count <= 0:
        raise ValueError('invalid workload size')
    rng = random.Random(seed)
    for _ in range(count):
        kind = rng.choice(('put', 'get', 'delete'))
        key = str(rng.randrange(key_count)).encode()
        value = rng.randbytes(rng.randrange(17)) if kind == 'put' else None
        yield Operation(kind, key, value)
