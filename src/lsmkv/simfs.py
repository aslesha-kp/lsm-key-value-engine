"""Deterministic inode/namespace crash simulator. See docs/design.md §17."""
from dataclasses import dataclass, field
from itertools import permutations, product
import random


@dataclass(frozen=True)
class CrashChoice:
    # Name (live or durable) -> number of pending content microsteps retained.
    prefixes: dict[str, int] = field(default_factory=dict)
    directory_order: tuple[int, ...] = ()
    directory_survives: bool | None = None


@dataclass(frozen=True)
class _Write:
    offset: int
    data: bytes

    @property
    def steps(self):
        return len(self.data)


@dataclass(frozen=True)
class _Truncate:
    size: int
    steps = 1


@dataclass
class _Inode:
    durable: bytes = b''
    pending: list[_Write | _Truncate] = field(default_factory=list)

    @property
    def steps(self):
        return sum(edit.steps for edit in self.pending)

    def contents(self, prefix=None):
        remaining = self.steps if prefix is None else prefix
        data = bytearray(self.durable)
        for edit in self.pending:
            if remaining == 0:
                break
            count = min(remaining, edit.steps)
            if isinstance(edit, _Write):
                if edit.offset > len(data):
                    data.extend(b'\0' * (edit.offset - len(data)))
                data[edit.offset:edit.offset + count] = edit.data[:count]
            else:
                if edit.size < len(data):
                    del data[edit.size:]
                else:
                    data.extend(b'\0' * (edit.size - len(data)))
            remaining -= count
        return bytes(data)

    @property
    def current(self):
        return self.contents()


class SimFS:
    sync_primitive = 'simulated-file-and-directory-barriers'

    def __init__(self, *, directory_exists=True):
        self._exists = directory_exists
        self._durable_exists = directory_exists
        self._closed = False
        self._files = {}
        self._live = {}
        self._durable = {}
        self._directory = []
        self._next = 0
        self._locks = {}

    def _check_open(self):
        if self._closed:
            raise ValueError('filesystem adapter is closed')

    def _check_directory(self):
        self._check_open()
        if not self._exists:
            raise FileNotFoundError('database directory')

    def directory_exists(self):
        self._check_open()
        return self._exists

    def ensure_directory(self):
        self._check_open()
        self._exists = True

    def sync_parent(self):
        self._check_directory()
        self._durable_exists = True

    def close(self):
        self._locks.clear()
        self._closed = True

    def reopen(self):
        self._closed = False

    def _name(self, name):
        if not isinstance(name, str) or not name or name in ('.', '..') or name.startswith('@inode:') or '/' in name or '\\' in name:
            raise ValueError('expected a single filename')
        return name

    def _number(self, name):
        self._name(name)
        if name not in self._live:
            raise FileNotFoundError(name)
        return self._live[name]

    def _inode(self, name):
        return self._files[self._number(name)]

    def create(self, name):
        self._check_directory()
        self._name(name)
        if name in self._live:
            raise FileExistsError(name)
        number = self._next
        self._next += 1
        self._files[number] = _Inode()
        self._live[name] = number
        self._directory.append({name: number})

    def read(self, name, offset=0, size=None):
        self._check_directory()
        if offset < 0 or (size is not None and size < 0):
            raise ValueError('negative read range')
        return self._inode(name).current[offset:None if size is None else offset + size]

    def write(self, name, data, offset=None):
        self._check_directory()
        inode = self._inode(name)
        if not isinstance(data, bytes):
            raise TypeError('data must be bytes')
        offset = len(inode.current) if offset is None else offset
        if offset < 0:
            raise ValueError('negative offset')
        if data:
            inode.pending.append(_Write(offset, data))

    def truncate(self, name, size):
        self._check_directory()
        if size < 0:
            raise ValueError('negative size')
        inode = self._inode(name)
        inode.pending.append(_Truncate(size))

    def listdir(self):
        self._check_directory()
        return sorted(self._live)

    def sync_file(self, name):
        self._check_directory()
        inode = self._inode(name)
        inode.durable = inode.current
        inode.pending.clear()

    def sync_dir(self):
        self._check_directory()
        self._durable = self._live.copy()
        self._directory.clear()

    def rename(self, source, destination):
        self._check_directory()
        self._name(destination)
        number = self._number(source)
        if self._live.get(destination) == number:
            return
        del self._live[source]
        self._live[destination] = number
        self._directory.append({source: None, destination: number})

    def unlink(self, name):
        self._check_directory()
        self._number(name)
        del self._live[name]
        self._directory.append({name: None})

    def lock(self, name):
        self._check_directory()
        self._name(name)
        if name in self._locks:
            raise BlockingIOError(name)
        token = object()
        self._locks[name] = token
        return token

    def unlock(self, token):
        self._check_directory()
        for name, held in self._locks.items():
            if held is token:
                del self._locks[name]
                return
        raise ValueError('invalid lock token')

    def _named_inodes(self):
        # Include overwritten/unlinked inodes still reachable through pending edits.
        reachable = set(self._durable.values()) | set(self._live.values())
        for edit in self._directory:
            reachable.update(n for n in edit.values() if n is not None)
        result = {number: f'@inode:{number}' for number in reachable}
        used = set()
        for entries in (self._live, self._durable):
            for name, number in sorted(entries.items()):
                if name not in used and result[number].startswith('@inode:'):
                    result[number] = name
                    used.add(name)
        return result

    def sample_crash(self, seed):
        rng = random.Random(seed)
        prefixes = {name: rng.randrange(self._files[n].steps + 1)
                    for n, name in sorted(self._named_inodes().items())}
        order = list(range(len(self._directory)))
        rng.shuffle(order)
        order = tuple(i for i in order if rng.getrandbits(1))
        survives = self._exists and (self._durable_exists or bool(rng.getrandbits(1)))
        return CrashChoice(prefixes, order, survives)

    def enumerate_crashes(self):
        """All microstep prefixes × ordered subsets; factorial, small cases only."""
        named = sorted(self._named_inodes().items())
        for counts in product(*(range(self._files[n].steps + 1) for n, _ in named)):
            prefixes = dict(zip((name for _, name in named), counts))
            for size in range(len(self._directory) + 1):
                for order in permutations(range(len(self._directory)), size):
                    for survives in self._root_outcomes():
                        yield CrashChoice(prefixes.copy(), order, survives)

    def _root_outcomes(self):
        if self._durable_exists:
            return (True,)
        return (False, True) if self._exists else (False,)

    def crash(self, choice=None):
        choice = choice or CrashChoice()
        survives = (self._durable_exists if choice.directory_survives is None
                    else choice.directory_survives)
        if survives not in self._root_outcomes():
            raise ValueError('invalid directory survival choice')
        named = self._named_inodes()
        if set(choice.prefixes) - set(named.values()):
            raise ValueError('unknown crash prefix name')
        if len(set(choice.directory_order)) != len(choice.directory_order) or any(
                i < 0 or i >= len(self._directory) for i in choice.directory_order):
            raise ValueError('invalid directory operation selection')
        selected = {}
        for number, inode in self._files.items():
            count = choice.prefixes.get(named.get(number), 0)
            if not 0 <= count <= inode.steps:
                raise ValueError('invalid content prefix')
            selected[number] = inode.contents(count)
        entries = self._durable.copy()
        for index in choice.directory_order:
            for name, number in self._directory[index].items():
                if number is None:
                    entries.pop(name, None)
                else:
                    entries[name] = number
        if not survives:
            entries = {}
        self._files = {number: _Inode(selected[number]) for number in set(entries.values())}
        self._exists = self._durable_exists = survives
        self._closed = False
        self._live = entries
        self._durable = entries.copy()
        self._directory.clear()
        self._locks.clear()
