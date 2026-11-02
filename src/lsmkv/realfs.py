"""Serial, directory-relative storage I/O for Linux and macOS.

The database's parent must already exist. Data descriptors are scoped to each
call; flock descriptors remain open until explicit unlock/close. No implicit sync.
"""
from contextlib import contextmanager
import errno
import os
import stat
import sys

try:
    import fcntl
except ImportError:  # Permit importing SimFS/CLI help on unsupported platforms.
    fcntl = None

from .errors import DurabilityUnavailableError

_UNAVAILABLE = {errno.EINVAL, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP}


class RealFS:
    directory_sync_primitive = 'os.fsync'
    parent_sync_primitive = 'os.fsync'

    def __init__(self, path):
        if sys.platform not in ('linux', 'darwin'):
            raise DurabilityUnavailableError(errno.ENOTSUP, 'RealFS requires Linux or macOS')
        if fcntl is None or not callable(getattr(fcntl, 'flock', None)):
            raise DurabilityUnavailableError(errno.ENOTSUP, 'required flock is unavailable')
        if not callable(getattr(os, 'fsync', None)):
            raise DurabilityUnavailableError(errno.ENOTSUP, 'required os.fsync is unavailable')
        for flag in ('O_DIRECTORY', 'O_NOFOLLOW', 'O_CLOEXEC'):
            if not hasattr(os, flag):
                raise DurabilityUnavailableError(errno.ENOTSUP, 'required ' + flag + ' is unavailable')
        self._fullsync = None
        if sys.platform == 'darwin':
            self._fullsync = getattr(fcntl, 'F_FULLFSYNC', None)
            if self._fullsync is None:
                raise DurabilityUnavailableError(errno.ENOTSUP, 'required macOS F_FULLFSYNC is unavailable')
            self.sync_primitive = 'fcntl.F_FULLFSYNC'
        else:
            self.sync_primitive = 'os.fsync'
        self.path = os.path.abspath(os.fspath(path))
        self._parent_path, self._basename = os.path.split(self.path)
        if not self._basename:
            raise ValueError('database must be a child directory, not filesystem root')
        self._parent_fd = None
        self._dir_fd = None
        self._locks = {}
        self._closed = True
        self._pid = os.getpid()
        self.reopen()

    def _check(self):
        if self._pid != os.getpid():
            raise RuntimeError('create a fresh RealFS after fork')
        if self._closed:
            raise ValueError('filesystem adapter is closed')

    @staticmethod
    def _name(name):
        if (not isinstance(name, str) or not name or name in ('.', '..')
                or name.startswith('@inode:') or any(c in name for c in ('/', '\\', '\0'))):
            raise ValueError('expected a single filename')
        return name

    @property
    def _directory_flags(self):
        return os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC

    def reopen(self):
        if self._pid != os.getpid():
            raise RuntimeError('create a fresh RealFS after fork')
        if not self._closed:
            return  # Never close another Engine's still-live lock descriptors.
        parent = os.open(self._parent_path, self._directory_flags)
        directory = None
        try:
            try:
                directory = os.open(self._basename, self._directory_flags | os.O_NOFOLLOW, dir_fd=parent)
            except FileNotFoundError:
                pass
        except BaseException:
            os.close(parent)
            raise
        self._parent_fd, self._dir_fd = parent, directory
        self._closed = False

    def directory_exists(self):
        self._check()
        try:
            info = os.stat(self._basename, dir_fd=self._parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not stat.S_ISDIR(info.st_mode):
            raise NotADirectoryError(self.path)
        return True

    def ensure_directory(self):
        self._check()
        try:
            os.mkdir(self._basename, mode=0o700, dir_fd=self._parent_fd)
        except FileExistsError:
            pass
        self._directory()

    def _directory(self):
        self._check()
        if self._dir_fd is None:
            self._dir_fd = os.open(self._basename, self._directory_flags | os.O_NOFOLLOW, dir_fd=self._parent_fd)
        return self._dir_fd

    def _open_file(self, name, flags):
        fd = os.open(self._name(name), flags | os.O_CLOEXEC | os.O_NOFOLLOW,
                     0o600, dir_fd=self._directory())
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(errno.EINVAL, 'storage entry must be a regular file', name)
        except BaseException:
            os.close(fd)
            raise
        return fd

    @contextmanager
    def _file(self, name, flags):
        fd = self._open_file(name, flags)
        try:
            yield fd
        finally:
            os.close(fd)

    def create(self, name):
        with self._file(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL):
            pass

    def read(self, name, offset=0, size=None):
        if offset < 0 or (size is not None and size < 0):
            raise ValueError('negative read range')
        chunks = []
        with self._file(name, os.O_RDONLY) as fd:
            os.lseek(fd, offset, os.SEEK_SET)
            remaining = size
            while remaining is None or remaining > 0:
                data = os.read(fd, 1024 * 1024 if remaining is None else min(remaining, 1024 * 1024))
                if not data:
                    break
                chunks.append(data)
                if remaining is not None:
                    remaining -= len(data)
        return b''.join(chunks)

    def write(self, name, data, offset=None):
        if not isinstance(data, bytes):
            raise TypeError('data must be bytes')
        if offset is not None and offset < 0:
            raise ValueError('negative offset')
        flags = os.O_WRONLY | (os.O_APPEND if offset is None else 0)
        with self._file(name, flags) as fd:
            if offset is not None:
                os.lseek(fd, offset, os.SEEK_SET)
            view = memoryview(data)
            position = 0
            while position < len(view):
                try:
                    count = os.write(fd, view[position:])
                except InterruptedError:
                    continue  # A write interrupted before accepting bytes.
                if count <= 0:
                    raise OSError(errno.EIO, 'write made no progress')
                position += count

    def truncate(self, name, size):
        if size < 0:
            raise ValueError('negative size')
        with self._file(name, os.O_WRONLY) as fd:
            os.ftruncate(fd, size)

    @staticmethod
    def _barrier(primitive, action):
        try:
            action()  # Never retry a failed sync or silently substitute a primitive.
        except OSError as error:
            if error.errno in _UNAVAILABLE:
                raise DurabilityUnavailableError(error.errno, 'required ' + primitive + ' is unavailable: ' + str(error)) from error
            raise

    def sync_file(self, name):
        with self._file(name, os.O_RDWR) as fd:
            if self._fullsync is not None:
                self._barrier(self.sync_primitive, lambda: fcntl.fcntl(fd, self._fullsync))
            else:
                self._barrier(self.sync_primitive, lambda: os.fsync(fd))

    def sync_dir(self):
        fd = self._directory()
        self._barrier('directory os.fsync', lambda: os.fsync(fd))

    def sync_parent(self):
        self._directory()  # Missing database directory must not be silently synced.
        self._barrier('parent-directory os.fsync', lambda: os.fsync(self._parent_fd))

    def listdir(self):
        return sorted(os.listdir(self._directory()))

    def rename(self, source, destination):
        directory = self._directory()
        # os.replace is atomic within this bound directory; MANIFEST is the
        # intentional replacement. Engine checks fresh SSTable destinations.
        os.replace(self._name(source), self._name(destination), src_dir_fd=directory, dst_dir_fd=directory)

    def unlink(self, name):
        os.unlink(self._name(name), dir_fd=self._directory())

    def lock(self, name):
        self._directory()
        self._name(name)
        if any(held_name == name for held_name, fd in self._locks.values()):
            raise BlockingIOError(errno.EWOULDBLOCK, 'lock already owned', name)
        fd = self._open_file(name, os.O_RDWR | os.O_CREAT)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in _UNAVAILABLE:
                    raise DurabilityUnavailableError(error.errno, 'required flock is unavailable') from error
                raise
            token = object()
            self._locks[token] = (name, fd)
            return token
        except BaseException:
            os.close(fd)
            raise

    def unlock(self, token):
        self._check()
        try:
            name, fd = self._locks.pop(token)
        except (KeyError, TypeError):
            raise ValueError('invalid lock token') from None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def close(self):
        if self._closed:
            return
        self._closed = True
        descriptors = [fd for name, fd in self._locks.values()]
        descriptors += [fd for fd in (self._dir_fd, self._parent_fd) if fd is not None]
        self._locks.clear()
        self._dir_fd = self._parent_fd = None
        error = None
        for fd in descriptors:
            try:
                os.close(fd)  # Releases flock; safe for inherited copies after fork.
            except OSError as failure:
                error = error or failure
        if error is not None:
            raise error

    def __del__(self):
        # Failed Engine.open(RealFS(path)) may leave no caller-held adapter.
        # Best-effort descriptor reclamation only; never an implicit sync.
        try:
            if not getattr(self, '_closed', True):
                self.close()
        except Exception:
            pass  # Explicit close/context management remains the error-reporting path.

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *exc):
        self.close()
