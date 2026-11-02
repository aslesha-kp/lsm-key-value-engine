"""Real process death, NOT simulated power loss: the kernel cache survives."""
import json
import os
from pathlib import Path
import random
import re
import select
import signal
import subprocess
import sys
import tempfile
import time

import pytest

from lsmkv import Engine
from lsmkv.model import History, Operation, generate_operations
from lsmkv.realfs import RealFS
from lsmkv.wal import Record, encode_record

pytestmark = [pytest.mark.processkill, pytest.mark.skipif(sys.platform not in ('linux', 'darwin'), reason='SIGKILL adapter tests require Linux/macOS')]
ROOT = Path(__file__).resolve().parents[1]
SEEDS = [int(s) for s in os.environ.get('LSMKV_KILL_SEEDS', '11,29').split(',')]


class Child:
    def __init__(self, directory, seed, case, report, *, step_open=False, history=None):
        self.directory, self.report = directory, report
        self.history = history or History()
        self.log = {'failure_model': 'process SIGKILL; OS cache survives', 'seed': seed, 'case': case,
                    'platform': sys.platform, 'python': sys.version, 'operations': [], 'events': []}
        self.stderr = tempfile.TemporaryFile()
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        args = [sys.executable, '-u', str(ROOT / 'tests' / 'process_worker.py'), str(directory)]
        if step_open: args.append('--step-open')
        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.stderr, env=env, close_fds=True)
        self.log['pid'] = self.proc.pid
        os.set_blocking(self.proc.stdin.fileno(), False)
        self.buffer = b''
        self.deadline = time.monotonic() + 90
        self.killed = False

    def timeout(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0: raise TimeoutError('child test exceeded 90 seconds')
        return min(10, remaining)

    def send(self, message):
        data = memoryview((json.dumps(message) + '\n').encode())
        while data:
            if not select.select([], [self.proc.stdin], [], self.timeout())[1]:
                raise TimeoutError('child command write timed out')
            try:
                count = os.write(self.proc.stdin.fileno(), data)
            except BlockingIOError:
                continue
            if count <= 0: raise RuntimeError('child command write made no progress')
            data = data[count:]

    def read(self):
        while b'\n' not in self.buffer:
            if not select.select([self.proc.stdout], [], [], self.timeout())[0]:
                raise TimeoutError('child response timed out')
            chunk = os.read(self.proc.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError('child exited before response: ' + str(self.proc.poll()))
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b'\n', 1)
        event = json.loads(line)
        self.log['events'].append(event)
        if event['event'] == 'error': raise RuntimeError(event['error'])
        return event

    def proceed(self, event):
        self.send({'event': 'continue', 'id': event['id'], 'call': event['call'], 'phase': event['phase']})

    def ready(self, target=None):
        while True:
            event = self.read()
            if event['event'] == 'adapter':
                self.log['primitives'] = event
                continue
            if event['event'] == 'ready':
                self.log['primitives'] = event
                return True
            assert event['event'] == 'fs' and event['id'] == 'open'
            if target and event['method'] == target[0] and event['phase'] == target[1]:
                self.kill(event); return False
            self.proceed(event)

    def kill(self, location):
        self.log['kill'] = location
        self.proc.send_signal(signal.SIGKILL)
        assert self.proc.wait(timeout=5) == -signal.SIGKILL
        self.killed = True

    def run(self, operations, target):
        self.log['planned_operations'] = operations
        self.log['max_outstanding'] = 0
        for index, command in enumerate(operations):
            op = {**command, 'id': index}
            self.log['operations'].append({**op, 'started': False, 'acknowledged': False})
            record = self.log['operations'][-1]
            self.send(op)
            assert self.read() == {'event': 'started', 'id': index}
            record['started'] = True
            outstanding = sum(op['started'] and not op['acknowledged'] for op in self.log['operations'])
            assert outstanding == 1
            self.log['max_outstanding'] = max(self.log['max_outstanding'], outstanding)
            kind = op['kind']
            if kind in ('put', 'delete', 'get'):
                self.history.begin(Operation(kind, bytes.fromhex(op['key']),
                                             bytes.fromhex(op['value']) if kind == 'put' else None))
            self.send({'event': 'run', 'id': index})
            if index == target['op'] and target['stage'] == 'running':
                time.sleep(target['delay'])
                self.kill({'id': index, 'stage': 'running', 'delay': target['delay'],
                           'exact_syscall': 'unknown; scheduler-dependent'})
                return
            matched = 0
            while True:
                event = self.read()
                assert event['id'] == index
                if event['event'] == 'fs':
                    if (index == target['op'] and target['stage'] == 'fs'
                            and event['method'] == target['method'] and event['phase'] == target['phase']):
                        matched += 1
                        if matched == target.get('occurrence', 1):
                            self.kill(event); return
                    self.proceed(event)
                elif event['event'] == 'completed':
                    record['completed'] = event
                    if index in (6, 9): assert event['table_count'] == (1 if index == 6 else 0)
                    if kind == 'get':
                        value = self.history.state.get(bytes.fromhex(op['key']))
                        assert event['value'] == (None if value is None else value.hex())
                    self.send({'event': 'ack', 'id': index})
                else:
                    assert event['event'] == 'acknowledged'
                    record['acknowledged'] = True
                    if self.history.inflight is not None: self.history.acknowledge()
                    if index == target['op'] and target['stage'] == 'acknowledged':
                        self.kill(event); return
                    break
        raise AssertionError('kill target not reached')

    def verify(self):
        assert self.killed
        fs = RealFS(self.directory)
        try:
            db = Engine.open(fs)
            recovered = db.snapshot()
            self.log['recovered'] = {key.hex(): value.hex() for key, value in recovered.items()}
            self.history.check(recovered)
            db.close()
        finally:
            fs.close()

    def close(self):
        # Always reap a live child, even when an assertion, timeout, or oracle fails.
        if self.proc.poll() is None:
            self.log.setdefault('kill', {'stage': 'cleanup', 'reason': 'abort or timeout'})
            self.proc.kill()
        self.proc.wait(timeout=5)
        self.log['returncode'] = self.proc.returncode
        self.log['acknowledged_state'] = {key.hex(): value.hex() for key, value in self.history.state.items()}
        if self.history.inflight:
            op = self.history.inflight
            self.log['inflight'] = {'kind': op.kind, 'key': op.key.hex(),
                                    'value': None if op.value is None else op.value.hex()}
        self.proc.stdin.close(); self.proc.stdout.close()
        self.stderr.seek(0); self.log['stderr'] = self.stderr.read().decode(errors='replace'); self.stderr.close()
        self.report.write_text(json.dumps(self.log, indent=2, sort_keys=True))
        artifacts = Path(os.environ.get('LSMKV_TEST_ARTIFACT_DIR', str(ROOT / '.test-artifacts')))
        artifacts.mkdir(parents=True, exist_ok=True)
        name = re.sub(r'[^A-Za-z0-9_-]', '_', f"sigkill-{self.log['case']}-{self.log['seed']}") + '.json'
        (artifacts / name).write_text(self.report.read_text())


def workload(seed):
    rng = random.Random(seed)
    def put(key, value): return {'kind': 'put', 'key': key.hex(), 'value': value.hex()}
    ops = [put(b'a', rng.randbytes(4)), put(b'b', b'b'), {'kind': 'flush'},
           put(b'a', rng.randbytes(8)), {'kind': 'delete', 'key': b'b'.hex()}, {'kind': 'flush'},
           {'kind': 'compact'}, {'kind': 'delete', 'key': b'a'.hex()}, {'kind': 'flush'}, {'kind': 'compact'}]
    for op in generate_operations(seed, 8, key_count=3):
        item = {'kind': op.kind, 'key': op.key.hex()}
        if op.value is not None: item['value'] = op.value.hex()
        ops.append(item)
    ops += [{'kind': 'flush'}, {'kind': 'compact'}]
    return ops


@pytest.mark.parametrize('seed', SEEDS)
@pytest.mark.parametrize('case', ['write', 'flush', 'compact-nonempty', 'compact-empty', 'acknowledged', 'free-running'])
def test_sigkill_whole_history_with_one_outstanding_operation(tmp_path, seed, case):
    ops = workload(seed)
    targets = {
        'write': {'op': 3, 'stage': 'fs', 'method': 'write', 'phase': 'after'},
        'flush': {'op': 2, 'stage': 'fs', 'method': 'rename', 'phase': 'after', 'occurrence': 2},
        'compact-nonempty': {'op': 6, 'stage': 'fs', 'method': 'rename', 'phase': 'after', 'occurrence': 2},
        'compact-empty': {'op': 9, 'stage': 'fs', 'method': 'unlink', 'phase': 'before'},
        'acknowledged': {'op': len(ops) - 1, 'stage': 'acknowledged'},
        'free-running': {'op': 3, 'stage': 'running', 'delay': random.Random(seed).uniform(0, 0.002)},
    }
    if case == 'free-running':
        ops[3]['free_running'] = True
        ops[3]['value'] = random.Random(seed).randbytes(128 * 1024).hex()
    child = Child(tmp_path / 'db', seed, case, tmp_path / 'report.json')
    try:
        assert child.ready()
        # A different real adapter cannot open while the child owns the lock.
        losing = RealFS(tmp_path / 'db')
        try:
            with pytest.raises(BlockingIOError): Engine.open(losing)
        finally:
            losing.close()
        child.log['target'] = targets[case]
        child.run(ops, targets[case]); child.verify()
    finally:
        child.close()


def test_sigkill_during_recovery_then_second_recovery(tmp_path):
    directory = tmp_path / 'db'
    fs = RealFS(directory); db = Engine.open(fs); db.put(b'ack', b'value'); name = db.active_wal; db.close()
    fs.reopen(); fs.write(name, encode_record(Record(2, 1, b'partial', b'tail'))[:-1]); fs.sync_file(name); fs.close()
    for index, target in enumerate((('truncate', 'after'), ('sync_file', 'before'))):
        history = History(); history.begin(Operation('put', b'ack', b'value')); history.acknowledge()
        child = Child(directory, 17, f'recovery-{index + 1}', tmp_path / f'recovery-{index + 1}.json',
                      step_open=True, history=history)
        try:
            assert not child.ready(target)
            # Do not recover between the two kills: the next child is recovery #2.
            assert child.killed
        finally:
            child.close()
    db = Engine.open(RealFS(directory)); assert db.snapshot() == {b'ack': b'value'}; db.close()


def test_timed_out_parent_always_kills_and_reaps_child(tmp_path):
    child = Child(tmp_path / 'db', 0, 'timeout-cleanup', tmp_path / 'timeout.json')
    try:
        assert child.ready()
        child.deadline = time.monotonic() + 0.02
        with pytest.raises(TimeoutError): child.read()
    finally:
        child.close()
    assert child.proc.poll() == -signal.SIGKILL
    db = Engine.open(RealFS(tmp_path / 'db')); assert db.snapshot() == {}; db.close()
