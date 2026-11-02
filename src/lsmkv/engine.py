"""Serial WAL/SSTable engine with ordered, durable flush installation."""
import re
from copy import deepcopy

from .errors import (ClosedError, CorruptionError, FailedStateError,
                     MissingManifestError, UnsupportedFormatError)
from .manifest import encode_initial, decode as decode_manifest, encode as encode_manifest, file_number
from .sstable import encode as encode_table, decode as decode_table
from .fs import FileSystem
from .wal import HEADER, MAX_SEQ, Record, encode_record, replay, validate_key

WAL_NAME = re.compile(r'wal-[0-9]{6,}\.log')
FIRST_WAL = 'wal-000001.log'


class Engine:
    def __init__(self, fs: FileSystem, token, memtable_bytes, wal_bytes):
        self.fs = fs
        self._token = token
        self._memtable = {}
        self._next_seq = 1
        self._active = FIRST_WAL
        self.failed = False
        self._closed = False
        self._memtable_limit = memtable_bytes
        self._wal_limit = wal_bytes
        self._live_bytes = 0
        self._wal_bytes = 0
        self._tables = []
        self._metadata = None
        self._wals = []
        self._wal_max = {}
        self._next_file = 2
        self.last_flush_wal_bytes = 0

    @property
    def next_seq(self):
        return self._next_seq

    @property
    def memtable(self):
        return self._memtable.copy()

    @property
    def active_wal(self):
        return self._active

    @property
    def live_bytes(self):
        return self._live_bytes

    @classmethod
    def open(cls, fs: FileSystem, *, memtable_bytes=4 * 1024 * 1024, wal_bytes=16 * 1024 * 1024):
        if any(type(limit) is not int or limit <= 0 for limit in (memtable_bytes, wal_bytes)):
            raise ValueError('flush thresholds must be positive integer byte counts')
        # Provisioning has no acknowledgment yet, but a visible unsynced root
        # from an earlier failed open must also be made durable.
        fs.reopen()
        fs.ensure_directory()
        fs.sync_parent()
        token = fs.lock('LOCK')  # Never close/unlock another opener on failure.
        engine = cls(fs, token, memtable_bytes, wal_bytes)
        try:
            names = set(fs.listdir())
            if 'LOCK' not in names:
                fs.create('LOCK')
            names.discard('LOCK')
            if 'MANIFEST' not in names:
                engine._initialize(names)
                names = set(fs.listdir()) - {'LOCK'}
            engine._metadata = decode_manifest(fs.read('MANIFEST'))
            wals = sorted((name for name in names if WAL_NAME.fullmatch(name) and file_number(name) is not None),
                          key=file_number)
            if not wals:
                raise CorruptionError('manifest exists without any WAL')
            known = set(wals) | {'MANIFEST'}
            if any(name not in known and not name.endswith('.tmp') and not name.startswith('sst-')
                   for name in names):
                raise UnsupportedFormatError('unrecognized database directory files')
            # Observe all numbered names before cleanup, including abandoned temps.
            engine._next_file = max([engine._metadata['next_file_number']] +
                                    [file_number(name) + 1 for name in names if file_number(name) is not None])
            for metadata in engine._metadata['sstables']:
                try:
                    raw = fs.read(metadata['name'])
                except FileNotFoundError as error:
                    raise CorruptionError('missing referenced SSTable: ' + metadata['name']) from error
                table = decode_table(raw)
                if (table.size != metadata['size'] or table.min_seq != metadata['min_seq']
                        or table.max_seq != metadata['max_seq']):
                    raise CorruptionError('SSTable metadata disagrees with manifest')
                engine._tables.append(table)
            # Validate every WAL before repairing tails or deleting any files.
            parsed, previous = [], 0
            for index, name in enumerate(wals):
                result = replay(fs.read(name), last=index == len(wals) - 1, previous_seq=previous)
                if result.records:
                    previous = result.records[-1].seq
                parsed.append(result)
            engine._recover_wals(wals, parsed)
            referenced = {table['name'] for table in engine._metadata['sstables']}
            garbage = [name for name in names if name.endswith('.tmp') or
                       (name.startswith('sst-') and name not in referenced)]
            # _recover_wals first stabilized the visible manifest/namespace.
            for name in sorted(garbage):
                fs.unlink(name)
            retired = engine._retire_wals()
            if garbage or retired:
                fs.sync_dir()
            return engine
        except Exception:
            # This open failed: release only the lock owned by this attempt.
            # InjectedCrash is a BaseException and must bypass cleanup I/O.
            engine.failed = True
            fs.unlock(token)
            raise

    def _initialize(self, names):
        fs = self.fs
        allowed = {'MANIFEST.tmp', FIRST_WAL}
        if names - allowed:
            raise MissingManifestError('missing MANIFEST in an existing database')
        if FIRST_WAL in names:
            data = fs.read(FIRST_WAL)
            if len(data) >= len(HEADER) and data != HEADER:
                raise MissingManifestError('missing MANIFEST beside invalid or nonempty WAL')
        else:
            fs.create(FIRST_WAL)
            data = b''
        if len(data) < len(HEADER):
            fs.truncate(FIRST_WAL, 0)
            fs.write(FIRST_WAL, HEADER)
        fs.sync_file(FIRST_WAL)
        fs.sync_dir()  # WAL bytes and filename precede any manifest install.
        if 'MANIFEST.tmp' not in names:
            fs.create('MANIFEST.tmp')
        fs.truncate('MANIFEST.tmp', 0)
        fs.write('MANIFEST.tmp', encode_initial())
        fs.sync_file('MANIFEST.tmp')
        fs.rename('MANIFEST.tmp', 'MANIFEST')
        fs.sync_dir()

    @staticmethod
    def _entry_bytes(key, entry):
        return 17 + len(key) + len(entry[2])

    def _recover_wals(self, names, parsed):
        highest = self._metadata['flushed_seq']
        for name, result in zip(names, parsed):
            self._wal_max[name] = result.records[-1].seq if result.records else 0
            for record in result.records:
                highest = max(highest, record.seq)
                if record.seq > self._metadata['flushed_seq']:
                    self._memtable[record.key] = (record.seq, record.op, record.value)
            if result.short_header:
                self.fs.truncate(name, 0)
                self.fs.write(name, HEADER)
                self.fs.sync_file(name)
                self.fs.sync_dir()
            elif result.incomplete:
                self.fs.truncate(name, result.valid_end)
                self.fs.sync_file(name)
        self._active = names[-1]
        self._wals = list(names)
        self._next_seq = highest + 1
        self._live_bytes = sum(self._entry_bytes(key, entry) for key, entry in self._memtable.items())
        self._wal_bytes = len(HEADER) if parsed[-1].short_header else parsed[-1].valid_end
        # A failed prior open/flush may leave a valid but unsynced namespace.
        # Commit it before any cleanup or WAL retirement uses its watermark.
        for name in names:
            self.fs.sync_file(name)
        self.fs.sync_dir()

    def _allocate(self, kind):
        number = self._next_file
        if number >= MAX_SEQ:
            raise OverflowError('file number space exhausted')
        self._next_file += 1
        name = f'wal-{number:06d}.log' if kind == 'wal' else f'sst-{number:06d}.sst'
        names = self.fs.listdir()
        if name in names or name + '.tmp' in names:
            raise FileExistsError('refusing file number reuse: ' + name)
        return name

    def _retire_wals(self):
        retired = []
        for name in list(self._wals):
            if name != self._active and self._wal_max[name] <= self._metadata['flushed_seq']:
                self.fs.unlink(name)
                retired.append(name)
                self._wals.remove(name)
                del self._wal_max[name]
        return retired

    def _install_manifest(self, metadata):
        self.fs.create('MANIFEST.tmp')  # exclusive; never truncate a possible alias
        self.fs.write('MANIFEST.tmp', encode_manifest(metadata))
        self.fs.sync_file('MANIFEST.tmp')
        self.fs.rename('MANIFEST.tmp', 'MANIFEST')
        self.fs.sync_dir()

    def flush(self):
        self._check()
        if not self._memtable:
            return
        try:
            frozen = self._memtable.copy()
            # 1. Install the next WAL before any SSTable or manifest change.
            new_wal = self._allocate('wal')
            self.fs.create(new_wal)
            self.fs.write(new_wal, HEADER)
            self.fs.sync_file(new_wal)
            self.fs.sync_dir()
            self.last_flush_wal_bytes = self._wal_bytes
            self._active = new_wal
            self._wals.append(new_wal)
            self._wal_max[new_wal] = 0
            self._wal_bytes = len(HEADER)
            # 2. Immutable table: fresh temp and final names, bytes then namespace.
            name = self._allocate('sst')
            raw = encode_table(frozen)
            self.fs.create(name + '.tmp')
            self.fs.write(name + '.tmp', raw)
            self.fs.sync_file(name + '.tmp')
            if name in self.fs.listdir():
                raise FileExistsError('refusing SSTable replacement: ' + name)
            self.fs.rename(name + '.tmp', name)
            self.fs.sync_dir()
            sequences = [entry[0] for entry in frozen.values()]
            metadata = deepcopy(self._metadata)
            metadata['generation'] += 1
            metadata['flushed_seq'] = max(sequences)
            metadata['next_file_number'] = self._next_file
            metadata['sstables'].append({'name': name, 'size': len(raw),
                                        'min_seq': min(sequences), 'max_seq': max(sequences)})
            # 3. Publish the new table only after its bytes and name are durable.
            self._install_manifest(metadata)
            self._metadata = metadata
            self._tables.append(decode_table(raw))
            self._memtable.clear()
            self._live_bytes = 0
            # 4. The new durable manifest now makes covered old WALs redundant.
            if self._retire_wals():
                self.fs.sync_dir()
        except Exception:
            self.failed = True
            raise

    def compact(self):
        """Merge every installed SSTable, without flushing the newer memtable."""
        self._check()
        if not self._tables:
            return
        try:
            inputs = [table['name'] for table in self._metadata['sstables']]
            newest = {}
            for table in self._tables:
                for key, entry in table.entries.items():
                    previous = newest.get(key)
                    if previous is None or entry[0] > previous[0]:
                        newest[key] = entry
            # All installed tables participated, so no older on-disk version
            # remains to be hidden by these tombstones. Memtable stays newer.
            merged = {key: entry for key, entry in newest.items() if entry[1] == 1}
            metadata = deepcopy(self._metadata)
            metadata['generation'] += 1
            metadata['sstables'] = []
            tables = []
            if merged:
                name = self._allocate('sst')
                raw = encode_table(merged)
                self.fs.create(name + '.tmp')
                self.fs.write(name + '.tmp', raw)
                self.fs.sync_file(name + '.tmp')
                if name in self.fs.listdir():
                    raise FileExistsError('refusing compaction SSTable replacement: ' + name)
                self.fs.rename(name + '.tmp', name)
                self.fs.sync_dir()
                table = decode_table(raw)
                metadata['sstables'] = [{'name': name, 'size': table.size,
                                         'min_seq': table.min_seq, 'max_seq': table.max_seq}]
                metadata['next_file_number'] = self._next_file
                tables = [table]
            # Empty output allocates nothing and preserves the manifest counter.
            # Neither branch changes flushed_seq, active WAL, or memtable state.
            self._install_manifest(metadata)
            self._metadata = metadata
            self._tables = tables
            # Only the durable replacement manifest makes every input redundant.
            for old_name in inputs:
                self.fs.unlink(old_name)
            self.fs.sync_dir()
        except Exception:
            self.failed = True
            raise

    def _check(self):
        if self._closed:
            raise ClosedError('engine is closed')
        if self.failed:
            raise FailedStateError('storage error: close and reopen to recover')

    def get(self, key):
        self._check()
        validate_key(key)
        entry = self._memtable.get(key)
        if entry is None:
            for table in reversed(self._tables):
                entry = table.entries.get(key)
                if entry is not None:
                    break
        return entry[2] if entry is not None and entry[1] == 1 else None

    def snapshot(self):
        """Diagnostic whole-state image, including keys unknown to the oracle."""
        self._check()
        versions = {}
        for table in self._tables:
            versions.update(table.entries)
        versions.update(self._memtable)
        return {key: entry[2] for key, entry in versions.items() if entry[1] == 1}

    def _write(self, op, key, value):
        self._check()
        if self._next_seq > MAX_SEQ:
            raise OverflowError('sequence number space exhausted')
        record = Record(self._next_seq, op, key, value)
        encoded = encode_record(record)  # Validate before sequence assignment/I/O.
        self._next_seq += 1
        try:
            self.fs.write(self._active, encoded)
            self.fs.sync_file(self._active)
        except OSError:
            self.failed = True
            raise
        old = self._memtable.get(key)
        if old is not None:
            self._live_bytes -= self._entry_bytes(key, old)
        self._memtable[key] = (record.seq, op, value)
        self._live_bytes += self._entry_bytes(key, self._memtable[key])
        self._wal_bytes += len(encoded)
        self._wal_max[self._active] = record.seq
        if self._live_bytes >= self._memtable_limit or self._wal_bytes >= self._wal_limit:
            self.flush()

    def put(self, key, value):
        self._write(1, key, value)

    def delete(self, key):
        self._write(2, key, b'')

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.fs.unlock(self._token)
        finally:
            self.fs.close()
