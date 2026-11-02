"""Small RealFS benchmark; run with python -m lsmkv.benchmark --help.

No cache eviction, engine tuning, or durability-policy changes. All verification,
workload generation, statistics, and report serialization are outside timers.
"""
import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
from time import perf_counter_ns

from .engine import Engine
from .manifest import decode as decode_manifest
from .realfs import RealFS
from .wal import MAX_VALUE


def digest(parts):
    value = hashlib.sha256()
    for part in parts:
        value.update(len(part).to_bytes(8, 'big'))
        value.update(part)
    return value.hexdigest()


def make_workload(*, seed, keys, value_bytes, rounds, queries, tail_keys):
    rng = random.Random(seed)
    names = [b'k' + index.to_bytes(7, 'big') for index in range(keys)]
    batches, expected = [], {}
    for generation in range(rounds):
        batch = []
        for index, key in enumerate(names):
            # Last generation deletes every fifth key; other generations overwrite.
            value = None if generation == rounds - 1 and generation > 0 and index % 5 == 0 else rng.randbytes(value_bytes)
            batch.append((key, value))
            if value is None:
                expected.pop(key, None)
            else:
                expected[key] = value
        batches.append(batch)
    tail = [(key, rng.randbytes(value_bytes)) for key in names[:tail_keys]]
    expected.update(tail)
    live = sorted(expected)
    existing_queries = [live[rng.randrange(len(live))] for _ in range(queries)]
    missing_queries = [b'm' + rng.randrange(keys).to_bytes(7, 'big') for _ in range(queries)]
    encoded = []
    for batch in [*batches, tail]:
        encoded.append(b'batch')
        for key, value in batch:
            encoded.extend((key, b'delete' if value is None else b'put', b'' if value is None else value))
    return {'batches': batches, 'tail': tail, 'expected': expected,
            'existing_queries': existing_queries, 'missing_queries': missing_queries,
            'fingerprint': digest(encoded),
            'query_fingerprint': digest([b'existing', *existing_queries, b'missing', *missing_queries]),
            'state_fingerprint': digest([part for item in sorted(expected.items()) for part in item])}


def verify(db, workload):
    # Equality catches missing keys, wrong values, and unexpected extra keys.
    if db.snapshot() != workload['expected']:
        raise AssertionError('benchmark recovered/visible state differs from expected whole state')
    for key in workload['existing_queries']:
        if db.get(key) != workload['expected'][key]:
            raise AssertionError('existing-key query returned incorrect bytes')
    for key in workload['missing_queries']:
        if db.get(key) is not None:
            raise AssertionError('missing-key query unexpectedly found a value')


def distribution(samples):
    ordered = sorted(samples)
    def percentile(fraction):
        return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]
    return {'samples_ns': samples, 'p50_ns': percentile(.50),
            'p95_ns': percentile(.95), 'p99_ns': percentile(.99)}


def lookup_times(db, workload):
    output = {}
    for kind in ('existing', 'missing'):
        samples = []
        for key in workload[kind + '_queries']:
            start = perf_counter_ns()
            db.get(key)
            elapsed = perf_counter_ns() - start
            samples.append(elapsed)
        output[kind] = distribution(samples)
    return output


def file_stats(fs):
    # Logical lengths, not allocated blocks. Use the storage interface throughout.
    sizes = {name: len(fs.read(name)) for name in fs.listdir()}
    metadata = decode_manifest(fs.read('MANIFEST'))
    return {'table_count': len(metadata['sstables']), 'file_sizes': sizes,
            'database_file_bytes': sum(sizes.values())}


def trial(path, workload, thresholds):
    fs = RealFS(path)
    db = None
    try:
        db = Engine.open(fs, **thresholds)
        # Throughput includes the unchanged per-put file sync and automatic flushes.
        initial = workload['batches'][0]
        start = perf_counter_ns()
        for key, value in initial:
            db.put(key, value)
        put_ns = perf_counter_ns() - start
        if db.snapshot() != dict(initial):
            raise AssertionError('initial durable-put state differs')
        db.flush()  # Explicit staging flushes are not charged to initial put time.
        for batch in workload['batches'][1:]:
            for key, value in batch:
                if value is None: db.delete(key)
                else: db.put(key, value)
            db.flush()
        for key, value in workload['tail']:
            db.put(key, value)
        verify(db, workload)
        output = {'directory': str(path), 'durable_put': {'count': len(initial),
                  'elapsed_ns': put_ns, 'operations_per_second': len(initial) * 1e9 / put_ns}}
        for stage in ('before', 'after'):
            db.close()
            db = None
            fs.reopen()  # Adapter construction/descriptor setup excluded from open timer.
            start = perf_counter_ns()
            db = Engine.open(fs, **thresholds)
            open_ns = perf_counter_ns() - start
            verify(db, workload)  # Also warms these exact queries before measurement.
            stats = file_stats(fs)
            stats['reopen_ns'] = open_ns
            stats['memtable_entries'] = len(db.memtable)
            stats['lookups'] = lookup_times(db, workload)
            verify(db, workload)
            output[stage] = stats
            if stage == 'before':
                start = perf_counter_ns()
                db.compact()
                output['compaction_ns'] = perf_counter_ns() - start
                verify(db, workload)
                output['immediate_post_compaction'] = file_stats(fs)
        output['verified'] = True
        return output
    finally:
        try:
            if db is not None: db.close()
        finally:
            fs.close()


def environment(work_dir):
    hardware = {'machine': platform.machine(), 'processor': platform.processor(),
                'logical_cpu_count': os.cpu_count(), 'physical_memory_bytes': None,
                'storage_device': 'not identified; work_dir and filesystem statistics recorded'}
    try:
        hardware['physical_memory_bytes'] = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
    except (ValueError, OSError):
        pass
    if sys.platform == 'darwin':
        for name, field in [('machdep.cpu.brand_string', 'cpu_model'), ('hw.model', 'hardware_model'),
                            ('hw.memsize', 'physical_memory_bytes')]:
            try:
                result = subprocess.run(['/usr/sbin/sysctl', '-n', name], text=True,
                                        capture_output=True, timeout=2)
                if result.returncode == 0:
                    hardware[field] = int(result.stdout) if field == 'physical_memory_bytes' else result.stdout.strip()
                else:
                    hardware[field + '_unavailable'] = result.stderr.strip()
            except (OSError, subprocess.TimeoutExpired) as error:
                hardware[field + '_unavailable'] = str(error)
    elif sys.platform == 'linux':
        try:
            for line in Path('/proc/cpuinfo').read_text().splitlines():
                if line.startswith(('model name', 'Hardware')):
                    hardware['cpu_model'] = line.split(':', 1)[1].strip()
                    break
        except OSError as error:
            hardware['cpu_model_unavailable'] = str(error)
    volume = os.statvfs(work_dir)
    source = Path(__file__).parent
    return {'python': sys.version, 'python_executable': sys.executable,
            'os': platform.platform(), 'uname': dict(platform.uname()._asdict()),
            'hardware': hardware, 'work_dir': str(work_dir), 'gc_enabled': gc.isenabled(),
            'filesystem': {'block_size': volume.f_frsize, 'total_bytes': volume.f_blocks * volume.f_frsize,
                           'available_bytes': volume.f_bavail * volume.f_frsize},
            'source_sha256': digest([p.name.encode() + p.read_bytes() for p in sorted(source.glob('*.py'))])}


def summary(runs):
    result = {'durable_put_ops_per_second_median': statistics.median(
                  run['durable_put']['operations_per_second'] for run in runs),
              'compaction_ms_median': statistics.median(run['compaction_ns'] / 1e6 for run in runs)}
    for stage in ('before', 'after'):
        result[stage] = {'reopen_ms_median': statistics.median(run[stage]['reopen_ns'] / 1e6 for run in runs),
                         'table_counts': [run[stage]['table_count'] for run in runs],
                         'database_file_bytes': [run[stage]['database_file_bytes'] for run in runs]}
        for kind in ('existing', 'missing'):
            result[stage][kind + '_lookup_ns'] = {
                quantile: statistics.median(run[stage]['lookups'][kind][quantile + '_ns'] for run in runs)
                for quantile in ('p50', 'p95', 'p99')}
    return result


def main(argv=None):
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--work-dir', type=Path, required=True, help='existing parent on target filesystem; run-NNN children must not exist')
    cli.add_argument('--output', type=Path, required=True, help='new raw JSON report (never overwritten)')
    cli.add_argument('--seed', type=int, default=20261102)
    for name, default in [('keys', 512), ('value-bytes', 256), ('rounds', 3), ('queries', 2000),
                          ('tail-keys', 32), ('repeats', 3), ('memtable-bytes', 65536), ('wal-bytes', 262144)]:
        cli.add_argument('--' + name, type=int, default=default)
    args = cli.parse_args(argv)
    if (args.keys < 2 or args.keys >= 2**56 or not 0 <= args.value_bytes <= MAX_VALUE
            or args.rounds < 2 or args.queries < 1 or not 0 <= args.tail_keys <= args.keys
            or args.repeats < 2 or min(args.memtable_bytes, args.wal_bytes) < 1):
        cli.error('require keys>=2, rounds/repeats>=2, queries/thresholds>0, valid value size, and 0<=tail-keys<=keys')
    try:
        work_dir = args.work_dir.resolve()
        if not work_dir.is_dir(): raise NotADirectoryError(work_dir)
        if args.output.exists(): raise FileExistsError(args.output)
        paths = [work_dir / f'run-{index + 1:03d}' for index in range(args.repeats)]
        for path in paths:
            if path.exists(): raise FileExistsError(path)
        parameters = {name: getattr(args, name) for name in ('seed', 'keys', 'value_bytes', 'rounds', 'queries',
                       'tail_keys', 'repeats', 'memtable_bytes', 'wal_bytes')}
        workload = make_workload(**{name: parameters[name] for name in
                                  ('seed', 'keys', 'value_bytes', 'rounds', 'queries', 'tail_keys')})
        raw = {'format_version': 1, 'started_utc': datetime.now(timezone.utc).isoformat(),
               'command': [sys.executable, '-m', 'lsmkv.benchmark', *(sys.argv[1:] if argv is None else argv)],
               'parameters': parameters, 'environment': environment(work_dir),
               'workload': {name: workload[name] for name in ('fingerprint', 'query_fingerprint', 'state_fingerprint')},
               'methodology': {'clock': 'time.perf_counter_ns', 'lookup_percentile': 'nearest rank',
                   'lookup_cache': 'SSTables cached in Python memory; verified/warmed queries; NOT cold disk reads',
                   'reopen_cache': 'same process, OS cache not evicted; includes Engine.open recovery syncs',
                   'put_policy': 'existing per-write sync, includes automatic flush; initialization and explicit staging flushes excluded',
                   'file_bytes': 'sum of logical file lengths including manifest, WALs, SSTables, LOCK; excludes directory metadata and allocated-block overhead',
                   'summary': 'median across fresh-directory repetitions of rates/times and per-run lookup percentiles; no pooled quantiles',
                   'verification': 'whole state and exact queries outside timers, same workload/queries in all runs and stages'},
               'runs': []}
        thresholds = {'memtable_bytes': args.memtable_bytes, 'wal_bytes': args.wal_bytes}
        for path in paths:
            # Selection metadata is available before provisioning; no storage I/O bypass.
            with RealFS(path) as adapter:
                raw['environment'].update({name: getattr(adapter, name) for name in
                                          ('sync_primitive', 'directory_sync_primitive', 'parent_sync_primitive')})
            raw['runs'].append(trial(path, workload, thresholds))
        raw['finished_utc'] = datetime.now(timezone.utc).isoformat()
        raw['summary'] = summary(raw['runs'])
        with args.output.open('x') as report:
            json.dump(raw, report, indent=2, sort_keys=True)
            report.write('\n')
        print(json.dumps({'output': str(args.output.resolve()), 'summary': raw['summary']}, indent=2))
        return 0
    except (OSError, ValueError, AssertionError) as error:
        print('benchmark failed: ' + str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
