"""Benchmark reproducibility and correctness checks, never speed thresholds."""
import json
import subprocess
import sys

import pytest

from lsmkv.benchmark import make_workload, verify


def test_fixed_workload_has_identical_queries_and_expected_membership():
    first = make_workload(seed=123, keys=12, value_bytes=16, rounds=3, queries=20, tail_keys=2)
    second = make_workload(seed=123, keys=12, value_bytes=16, rounds=3, queries=20, tail_keys=2)
    assert first == second
    assert first['fingerprint'] != make_workload(
        seed=124, keys=12, value_bytes=16, rounds=3, queries=20, tail_keys=2)['fingerprint']
    assert len(first['existing_queries']) == len(first['missing_queries']) == 20
    assert all(key in first['expected'] for key in first['existing_queries'])
    assert all(key not in first['expected'] for key in first['missing_queries'])
    assert all(len(value) == 16 for value in first['expected'].values())


def test_verification_rejects_unexpected_extra_keys():
    from types import SimpleNamespace
    workload = make_workload(seed=1, keys=4, value_bytes=8, rounds=2, queries=4, tail_keys=1)
    recovered = dict(workload['expected'])
    recovered[b'unexpected'] = b'bad'
    db = SimpleNamespace(snapshot=lambda: recovered, get=workload['expected'].get)
    with pytest.raises(AssertionError, match='whole state'):
        verify(db, workload)


@pytest.mark.realfs
@pytest.mark.skipif(sys.platform not in ('linux', 'darwin'), reason='RealFS requires Linux/macOS')
def test_realfs_benchmark_saves_repeated_raw_results_and_refuses_reuse(tmp_path):
    output = tmp_path / 'result.json'
    command = [sys.executable, '-m', 'lsmkv.benchmark', '--work-dir', str(tmp_path),
               '--output', str(output), '--keys', '12', '--value-bytes', '16',
               '--rounds', '3', '--queries', '20', '--tail-keys', '2', '--repeats', '2']
    result = subprocess.run(command, text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    raw = json.loads(output.read_text())
    assert len(raw['runs']) == 2
    assert raw['parameters']['seed'] == 20261102
    assert raw['environment']['sync_primitive'] in ('os.fsync', 'fcntl.F_FULLFSYNC')
    for run in raw['runs']:
        assert run['verified'] is True
        assert run['durable_put']['count'] == 12
        assert run['before']['table_count'] == 3
        assert run['after']['table_count'] == 1
        assert run['before']['database_file_bytes'] > run['after']['database_file_bytes']
        for stage in ('before', 'after'):
            assert run[stage]['reopen_ns'] > 0
            for kind in ('existing', 'missing'):
                latency = run[stage]['lookups'][kind]
                assert len(latency['samples_ns']) == 20
                assert 0 < latency['p50_ns'] <= latency['p95_ns'] <= latency['p99_ns']
    original = output.read_bytes()
    rerun = subprocess.run(command, text=True, capture_output=True, timeout=30)
    assert rerun.returncode != 0
    assert output.read_bytes() == original
