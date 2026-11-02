import errno
import os
import sys

import pytest

from lsmkv import Engine, CorruptionError
from lsmkv.errors import DurabilityUnavailableError
from lsmkv.realfs import RealFS
from lsmkv.harness import FaultFS, FaultPlan, InjectedCrash
from lsmkv.manifest import decode as decode_manifest
from lsmkv.wal import HEADER, Record, encode_record

pytestmark = [pytest.mark.realfs, pytest.mark.skipif(sys.platform not in ('darwin', 'linux'), reason='RealFS supports Linux/macOS only')]


def test_real_directory_lifecycle_exclusive_create_and_io(tmp_path):
    fs = RealFS(tmp_path / 'db')
    assert not fs.directory_exists()
    fs.ensure_directory(); fs.sync_parent()
    fs.create('a')
    with pytest.raises(FileExistsError): fs.create('a')
    fs.write('a', b'abc'); fs.write('a', b'XY', 1); fs.write('a', b'!')
    assert fs.read('a') == b'aXY!' and fs.read('a', 1, 2) == b'XY'
    fs.truncate('a', 6); assert fs.read('a') == b'aXY!\0\0'
    fs.sync_file('a'); fs.sync_dir()
    fs.create('b'); fs.write('b', b'old'); fs.sync_file('b')
    fs.rename('a', 'b'); fs.sync_dir()
    assert fs.listdir() == ['b'] and fs.read('b') == b'aXY!\0\0'
    fs.unlink('b'); fs.sync_dir(); fs.close(); fs.close()
    with pytest.raises(ValueError): fs.listdir()
    fs.reopen(); assert fs.listdir() == []; fs.close()


def test_short_and_interrupted_writes_are_completed(tmp_path, monkeypatch):
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory(); fs.create('a')
    write = os.write; calls = []
    def short(fd, data):
        calls.append(len(data))
        if len(calls) == 1: raise InterruptedError()
        return write(fd, data[:2])
    monkeypatch.setattr(os, 'write', short)
    fs.write('a', b'1234567')
    assert fs.read('a') == b'1234567' and len(calls) == 5
    monkeypatch.setattr(os, 'write', lambda fd, data: 0)
    with pytest.raises(OSError): fs.write('a', b'never')
    fs.close()


def test_competing_adapters_and_safe_lock_descriptor_lifecycle(tmp_path):
    first, second = RealFS(tmp_path / 'db'), RealFS(tmp_path / 'db')
    first.ensure_directory(); second.ensure_directory()
    token = first.lock('LOCK')
    with pytest.raises(BlockingIOError): second.lock('LOCK')
    second.close()
    # Closing a losing opener must not release the first owner's lock.
    second.reopen()
    with pytest.raises(BlockingIOError): second.lock('LOCK')
    first.unlock(token)
    other = second.lock('LOCK')
    with pytest.raises(ValueError): second.unlock(token)
    second.close(); first.close()
    first.reopen(); first.lock('LOCK'); first.close()
    second.reopen(); second.lock('LOCK'); second.close()


def test_same_adapter_second_engine_does_not_release_first_lock(tmp_path):
    fs = RealFS(tmp_path / 'db'); first = Engine.open(fs)
    with pytest.raises(BlockingIOError): Engine.open(fs)
    first.put(b'k', b'v'); first.close()
    db = Engine.open(fs); assert db.get(b'k') == b'v'; db.close()


def test_real_engine_flush_compact_reopen_and_corruption_rejection(tmp_path):
    fs = RealFS(tmp_path / 'db'); db = Engine.open(fs, memtable_bytes=35, wal_bytes=150)
    db.put(b'k', b'old'); db.flush(); db.put(b'k', b'new'); db.put(b'', b'')
    db.flush(); db.delete(b'k'); db.flush(); db.compact()
    assert db.snapshot() == {b'': b''}
    db.close(); db = Engine.open(fs)
    assert db.snapshot() == {b'': b''}
    db.close(); fs.reopen()
    table = decode_manifest(fs.read('MANIFEST'))['sstables'][0]['name']
    fs.write(table, b'?', 0); fs.sync_file(table)
    fs.create('orphan.tmp'); fs.write('orphan.tmp', b'keep'); fs.sync_dir()
    with pytest.raises(CorruptionError): Engine.open(fs)
    assert fs.read('orphan.tmp') == b'keep'
    # A failed open must release the lock even when validation fails.
    token = fs.lock('LOCK'); fs.unlock(token); fs.close()


def test_real_empty_compaction_and_sequence_watermark(tmp_path):
    fs = RealFS(tmp_path / 'db'); db = Engine.open(fs)
    db.put(b'k', b'v'); db.flush(); db.delete(b'k'); db.flush(); db.compact(); db.close()
    db = Engine.open(fs); assert db.snapshot() == {} and db.next_seq == 3
    db.put(b'k', b'reborn'); db.close()
    db = Engine.open(fs); assert db.get(b'k') == b'reborn' and db.next_seq == 4; db.close()


def test_interrupted_real_tail_recovery_reopens_safely(tmp_path):
    path = tmp_path / 'db'
    fs = RealFS(path); db = Engine.open(fs); db.put(b'k', b'ack'); active = db.active_wal; db.close()
    fs.reopen(); fs.write(active, encode_record(Record(2, 1, b'z', b'tail'))[:-1]); fs.sync_file(active); fs.close()
    trace = FaultFS(RealFS(path)); db = Engine.open(trace); db.close()
    target = next(e.call for e in trace.events if e.method == 'truncate')
    # Restore a valid incomplete tail, then interrupt immediately after repair.
    fs = RealFS(path); fs.write(active, encode_record(Record(2, 1, b'z', b'tail'))[:-1]); fs.sync_file(active); fs.close()
    raw = RealFS(path); io = FaultFS(raw, FaultPlan(target, 'after', 'crash'))
    with pytest.raises(InjectedCrash): Engine.open(io)
    raw.close()  # Simulate descriptor cleanup only; this does not discard OS cache.
    db = Engine.open(RealFS(path)); assert db.snapshot() == {b'k': b'ack'}; db.close()


def test_sync_primitive_selection_and_unavailable_policy(tmp_path, monkeypatch):
    import lsmkv.realfs as module
    monkeypatch.setattr(module.sys, 'platform', 'darwin')
    monkeypatch.delattr(module.fcntl, 'F_FULLFSYNC', raising=False)
    with pytest.raises(DurabilityUnavailableError, match='F_FULLFSYNC'): RealFS(tmp_path / 'db')


def test_macos_file_sync_never_falls_back(tmp_path, monkeypatch):
    import lsmkv.realfs as module
    monkeypatch.setattr(module.sys, 'platform', 'darwin')
    monkeypatch.setattr(module.fcntl, 'F_FULLFSYNC', 51, raising=False)
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory(); fs.create('a')
    calls = []
    def unavailable(fd, command, *args):
        calls.append(command)
        raise OSError(errno.ENOTSUP, 'not supported')
    monkeypatch.setattr(module.fcntl, 'fcntl', unavailable)
    monkeypatch.setattr(os, 'fsync', lambda fd: pytest.fail('forbidden file-sync fallback'))
    with pytest.raises(DurabilityUnavailableError): fs.sync_file('a')
    assert calls == [51] and fs.sync_primitive == 'fcntl.F_FULLFSYNC'
    fs.close()


@pytest.mark.parametrize('operation', ['sync_dir', 'sync_parent'])
def test_unavailable_directory_barriers_are_explicit(tmp_path, monkeypatch, operation):
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory()
    def fail(fd): raise OSError(errno.EINVAL, 'unsupported directory fsync')
    monkeypatch.setattr(os, 'fsync', fail)
    with pytest.raises(DurabilityUnavailableError, match='fsync'): getattr(fs, operation)()
    fs.close()


@pytest.mark.parametrize('name', ['', '.', '..', '../a', 'a/b', 'a\\b', '@inode:1', 'nul\0'])
def test_adapter_names_cannot_escape_directory(tmp_path, name):
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory()
    with pytest.raises(ValueError): fs.create(name)
    fs.close()


def test_failed_inline_engine_open_does_not_leak_directory_descriptors(tmp_path, monkeypatch):
    import gc
    path = tmp_path / 'db'
    fs = RealFS(path); db = Engine.open(fs); db.close()
    fs.reopen(); fs.write('MANIFEST', b'?', 0); fs.sync_file('MANIFEST'); fs.close()
    opened, closed = os.open, os.close
    live = set()
    def track_open(*args, **kwargs):
        fd = opened(*args, **kwargs); live.add(fd); return fd
    def track_close(fd):
        live.discard(fd); return closed(fd)
    def attempt():
        try:
            Engine.open(RealFS(path))
        except CorruptionError:
            pass
    with monkeypatch.context() as patch:
        patch.setattr(os, 'open', track_open); patch.setattr(os, 'close', track_close)
        attempt(); gc.collect()
        try:
            assert not live, f'failed inline open leaked descriptors: {live}'
        finally:
            for fd in list(live): track_close(fd)


def test_linux_selection_uses_fsync_and_not_fullfsync(tmp_path, monkeypatch):
    import lsmkv.realfs as module
    monkeypatch.setattr(module.sys, 'platform', 'linux')
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory(); fs.create('a')
    calls = []
    monkeypatch.setattr(os, 'fsync', lambda fd: calls.append(fd))
    monkeypatch.setattr(module.fcntl, 'fcntl', lambda *args: pytest.fail('Linux must select fsync'))
    fs.sync_file('a')
    assert len(calls) == 1 and fs.sync_primitive == 'os.fsync'
    fs.close()


def test_lock_failure_closes_its_descriptor(tmp_path, monkeypatch):
    import lsmkv.realfs as module
    fs = RealFS(tmp_path / 'db'); fs.ensure_directory()
    captured = []
    def fail(fd, operation):
        captured.append(fd); raise OSError(errno.EIO, 'lock failure')
    monkeypatch.setattr(module.fcntl, 'flock', fail)
    with pytest.raises(OSError): fs.lock('LOCK')
    assert len(captured) == 1
    with pytest.raises(OSError): os.fstat(captured[0])
    fs.close()


def test_descriptors_are_not_inheritable_and_close_releases_only_own_locks(tmp_path):
    first = RealFS(tmp_path / 'db'); first.ensure_directory()
    token = first.lock('LOCK')
    assert not any(os.get_inheritable(fd) for fd in
                   [first._parent_fd, first._dir_fd, first._locks[token][1]])
    # Opening/closing an unrelated descriptor for LOCK cannot drop its flock.
    first.read('LOCK')
    second = RealFS(tmp_path / 'db')
    with pytest.raises(BlockingIOError): second.lock('LOCK')
    second.close(); first.close()
