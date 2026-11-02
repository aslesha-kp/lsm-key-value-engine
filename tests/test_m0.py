import pytest
from lsmkv.simfs import SimFS, CrashChoice
from lsmkv.model import Operation, History, generate_operations


def baseline():
    fs = SimFS()
    fs.create('a')
    fs.write('a', b'old')
    fs.sync_file('a')
    fs.sync_dir()
    return fs


def test_synced_and_unsynced_contents():
    fs = baseline()
    fs.write('a', b'new', 0)
    fs.crash()
    assert fs.read('a') == b'old'
    fs.write('a', b'new', 0)
    fs.sync_file('a')
    fs.crash()
    assert fs.read('a') == b'new'


def test_file_and_directory_barriers_are_independent():
    fs = SimFS()
    fs.create('a'); fs.write('a', b'x'); fs.sync_file('a')
    fs.crash()
    assert fs.listdir() == []
    fs.create('b'); fs.write('b', b'x'); fs.sync_dir()
    fs.crash()
    assert fs.read('b') == b''


@pytest.mark.parametrize('prefix', range(5))
def test_every_append_prefix(prefix):
    fs = baseline()
    fs.write('a', b'1234', 3)
    fs.crash(CrashChoice({'a': prefix}))
    assert fs.read('a') == b'old' + b'1234'[:prefix]


@pytest.mark.parametrize('persist', [False, True])
def test_rename_delete_and_truncate(persist):
    fs = baseline()
    fs.rename('a', 'b')
    fs.crash(CrashChoice(directory_order=(0,) if persist else ()))
    assert fs.listdir() == (['b'] if persist else ['a'])
    name = fs.listdir()[0]
    fs.truncate(name, 1)
    fs.crash(CrashChoice({name: int(persist)}))
    assert fs.read(name) == (b'o' if persist else b'old')
    fs.unlink(name)
    fs.crash(CrashChoice(directory_order=(0,) if persist else ()))
    assert fs.listdir() == ([] if persist else [name])


def test_synced_namespace_replacement_and_repeated_recovery():
    fs = baseline()
    fs.create('tmp'); fs.write('tmp', b'new'); fs.sync_file('tmp')
    fs.rename('tmp', 'a'); fs.sync_dir(); fs.crash()
    for _ in range(3):
        fs.truncate('a', 1)
        fs.crash()
        assert fs.read('a') == b'new'
    fs.truncate('a', 2); fs.sync_file('a'); fs.crash(); fs.crash()
    assert fs.read('a') == b'ne'
    fs.unlink('a'); fs.sync_dir(); fs.crash()
    assert fs.listdir() == []


def test_locks_and_io():
    fs = baseline()
    token = fs.lock('LOCK')
    with pytest.raises(BlockingIOError): fs.lock('LOCK')
    fs.unlock(token)
    fs.lock('LOCK'); fs.crash(); fs.lock('LOCK')
    assert fs.read('a', 1, 1) == b'l'


def test_history_overwrite_delete_and_single_inflight():
    h = History()
    for op in [Operation('put', b'k', b'1'), Operation('put', b'k', b'2'), Operation('delete', b'k')]:
        h.begin(op); h.acknowledge()
    h.check({})
    h.begin(Operation('put', b'z', b''))
    h.check({}); h.check({b'z': b''})
    with pytest.raises(AssertionError): h.check({b'k': b'1'})
    with pytest.raises(RuntimeError): h.begin(Operation('delete', b'z'))
    h.recover({b'z': b''}); h.check({b'z': b''})


@pytest.mark.parametrize('skip', ['file', 'directory'])
def test_missing_barrier_is_detected(skip):
    fs = SimFS()
    fs.create('ack'); fs.write('ack', b'value')
    if skip != 'file': fs.sync_file('ack')
    if skip != 'directory': fs.sync_dir()
    fs.crash()
    h = History(); h.begin(Operation('put', b'k', b'value')); h.acknowledge()
    recovered = {b'k': fs.read('ack')} if 'ack' in fs.listdir() else {}
    assert recovered == ({b'k': b''} if skip == 'file' else {})
    with pytest.raises(AssertionError): h.check(recovered)


def test_seeded_generation_and_crashes():
    assert list(generate_operations(42, 100)) == list(generate_operations(42, 100))
    assert {o.kind for o in generate_operations(42, 100)} == {'put', 'get', 'delete'}
    for seed in range(20):
        fs = baseline(); fs.write('a', b'abc', 3)
        choice = fs.sample_crash(seed)
        other = baseline(); other.write('a', b'abc', 3)
        fs.crash(choice); other.crash(other.sample_crash(seed))
        assert fs.read('a') == other.read('a')


def test_overwritten_inode_remains_selectable_for_crash():
    fs = baseline()
    fs.write('a', b'!', 3)
    fs.create('tmp'); fs.write('tmp', b'new'); fs.sync_file('tmp')
    fs.rename('tmp', 'a')
    # Dropping both directory operations recovers the original inode, including
    # its explicitly selected unsynced append. It must remain addressable.
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 10  # 2 byte prefixes × 5 ordered subsets
    fs.crash(next(c for c in choices if c.directory_order == () and 1 in c.prefixes.values()))
    assert fs.read('a') == b'old!'


def test_enumerated_directory_orders_and_atomic_rename():
    outcomes = set()
    fs = baseline(); fs.rename('a', 'b'); fs.unlink('b')
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 5
    for choice in choices:
        fs = baseline(); fs.rename('a', 'b'); fs.unlink('b')
        fs.crash(choice)
        outcomes.add(tuple(fs.listdir()))
    assert outcomes == {('a',), ('b',), ()}


def test_complete_install_survives_every_enumerated_crash():
    fs = baseline()
    fs.create('tmp'); fs.write('tmp', b'new'); fs.sync_file('tmp')
    fs.rename('tmp', 'a'); fs.sync_dir()
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 1
    fs.crash(choices[0])
    assert fs.read('a') == b'new'


def test_invalid_crash_choice_does_not_modify_state():
    fs = baseline(); fs.write('a', b'x')
    with pytest.raises(ValueError): fs.crash(CrashChoice({'a': 2}))
    assert fs.read('a') == b'oldx'


def test_reference_reads_and_overwrite_inflight_are_whole_states():
    h = History()
    h.begin(Operation('put', b'k', b'old')); h.acknowledge()
    h.begin(Operation('get', b'k'))
    assert h.acknowledge() == b'old'
    h.begin(Operation('put', b'k', b'new'))
    h.check({b'k': b'old'}); h.check({b'k': b'new'})
    with pytest.raises(AssertionError): h.check({b'k': b'n'})
    h.recover({b'k': b'new'})
    h.begin(Operation('delete', b'k'))
    h.check({b'k': b'new'}); h.check({})
    h.recover({})
    h.begin(Operation('delete', b'absent')); h.acknowledge(); h.check({})


def test_missing_files_report_filesystem_errors():
    fs = SimFS()
    for action in (lambda: fs.read('missing'), lambda: fs.unlink('missing'),
                   lambda: fs.rename('missing', 'b')):
        with pytest.raises(FileNotFoundError): action()


def test_extend_truncation_and_partial_overwrite():
    fs = baseline(); fs.truncate('a', 5); fs.sync_file('a'); fs.crash()
    assert fs.read('a') == b'old\0\0'
    fs.write('a', b'NEW', 0); fs.crash(CrashChoice({'a': 2}))
    assert fs.read('a') == b'NEd\0\0'


@pytest.mark.parametrize('replace', [False, True])
def test_isolated_rename_is_atomic_in_every_crash_outcome(replace):
    fs = baseline()
    if replace:
        fs.create('b'); fs.write('b', b'other'); fs.sync_file('b'); fs.sync_dir()
    before = {name: fs.read(name) for name in fs.listdir()}
    fs.rename('a', 'b')
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 2
    from copy import deepcopy
    images = []
    for choice in choices:
        trial = deepcopy(fs); trial.crash(choice)
        images.append({name: trial.read(name) for name in trial.listdir()})
    assert images == [before, {'b': b'old'}]


def test_completed_barriers_with_pending_work_all_outcomes_and_second_crashes():
    from copy import deepcopy
    fs = baseline()
    fs.rename('a', 'installed'); fs.sync_dir()
    fs.create('victim'); fs.sync_dir(); fs.unlink('victim'); fs.sync_dir()
    fs.create('scratch'); fs.write('scratch', b'x')
    fs.rename('scratch', 'scratch2')
    fs.write('installed', b'yz')
    choices = list(fs.enumerate_crashes())
    assert len(choices) == 30  # 3 append prefixes × 2 scratch prefixes × 5 edit orders
    for choice in choices:
        trial = deepcopy(fs); trial.crash(choice)
        assert trial.read('installed') in (b'old', b'oldy', b'oldyz')
        assert 'a' not in trial.listdir()
        assert 'victim' not in trial.listdir()
        image = {name: trial.read(name) for name in trial.listdir()}
        # The surviving image is the new baseline, including unsynced survivors.
        for second_choice in trial.enumerate_crashes():
            second = deepcopy(trial); second.crash(second_choice)
            assert {name: second.read(name) for name in second.listdir()} == image


def test_rename_between_names_of_same_inode_is_noop():
    fs = baseline()
    fs.rename('a', 'b'); fs.rename('b', 'c')
    fs.crash(CrashChoice(directory_order=(1, 0)))
    assert fs.listdir() == ['b', 'c']
    fs.rename('b', 'c')
    assert fs.listdir() == ['b', 'c']
    fs.sync_dir(); fs.crash()
    assert fs.listdir() == ['b', 'c']
