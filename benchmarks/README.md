# RealFS benchmarks

The benchmark uses the existing per-write WAL sync policy. It records raw timings,
workload fingerprints, environment metadata, and whole-state verification results.

## Reproduce

Install the package as described in the project README, then run from the root:

```sh
benchmark_dir="$(mktemp -d './benchmarks/work-repro.XXXXXX')"
.venv/bin/python -m lsmkv.benchmark \
  --work-dir "$benchmark_dir" --output benchmarks/results/local-512.json \
  --seed 20261102 --keys 512 --value-bytes 256 --rounds 3 \
  --queries 2000 --tail-keys 32 --repeats 3 \
  --memtable-bytes 65536 --wal-bytes 262144
```

For the larger dataset, change only the key count and fresh paths:

```sh
benchmark_dir="$(mktemp -d './benchmarks/work-repro.XXXXXX')"
.venv/bin/python -m lsmkv.benchmark \
  --work-dir "$benchmark_dir" --output benchmarks/results/local-4096.json \
  --seed 20261102 --keys 4096 --value-bytes 256 --rounds 3 \
  --queries 2000 --tail-keys 32 --repeats 3 \
  --memtable-bytes 65536 --wal-bytes 262144
```

Use a new output filename on each invocation; reports are never overwritten.
The work directory must exist on the target filesystem. Its run-001, run-002,
etc. children must not exist. Each repetition uses a fresh database, retained
for inspection. Failed runs may leave directories but do not publish a successful
report. Work directories are ignored; raw JSON measurement records are retained.

## Workload

The fixed seed generates 8-byte keys (`k` plus a 7-byte big-endian index) and
256-byte values. Round one inserts every key, round two overwrites every key,
and round three deletes indices divisible by five and overwrites the rest.
Each round ends with an explicit flush; automatic threshold flushes remain
active. Finally, 32 keys receive newer values without an explicit flush.

The 512-key workload has 1,568 mutations, 103 deletions, and 416 final live keys.
The 4,096-key workload has 12,320 mutations, 820 deletions, and 3,283 final live
keys. Both recorded workloads leave 32 memtable/active-WAL entries. Other
thresholds can flush these entries; actual counts are recorded.

Each repetition reuses identical operations and ordered query lists. Existing
keys are sampled uniformly with replacement from the expected state. Missing
keys use a disjoint `m` prefix. Both stages run 2,000 existing queries followed
by 2,000 missing queries, with identical queries before/after compaction.

## Measurement methodology

- **Durable puts:** time the initial insert loop, including per-write sync and
  automatic flushes. Exclude initialization, generation, verification, later
  rounds, and explicit staging flushes. Throughput is count divided by time.
- **Clean reopen:** close and reopen the adapter outside the timer, then time
  Engine.open. This includes locking, validation, SSTable loading, WAL replay,
  and recovery barriers. Repeat after compaction. It is not a crash-recovery test.
- **Cached lookups:** verify whole state and exact query answers before timing,
  warming those queries. Time individual gets with time.perf_counter_ns and
  verify again afterward. Call/timer overhead is included; no GC changes,
  overhead subtraction, outlier filtering, or cache eviction is performed.
  Percentiles use nearest rank: sorted[ceil(p*N)-1].
- **Compaction:** time full db.compact, including output/manifest installation
  and input retirement. Check whole state afterward. Collect manifest table
  counts and per-file logical lengths through RealFS outside timers, before,
  immediately after, and after reopening. Bytes include WAL, manifest, SSTables,
  and LOCK; exclude directory metadata, block overhead, and peak temporary space.

Generation, verification, metadata collection, statistics, and serialization
are outside timers. Summary values are medians across three fresh-directory
repetitions; lookup summaries are medians of per-run percentiles, not pooled
quantiles or confidence intervals.

**SSTables are cached as Python dictionaries; lookups are not cold-disk reads.**
Reopen also retains OS caches. Background activity and cache history are
uncontrolled. These small workloads do not establish general performance scaling.

## Results

macOS 15.7.9 / Darwin 24.6.0 arm64, Python 3.14.7, 10 logical CPUs, 16 GiB RAM.
File barriers use F_FULLFSYNC; directory/parent barriers use fsync. CPU and storage
device models were unavailable; raw records preserve available metadata and
collection errors. All three repetitions of each workload passed verification.

| Measurement | 512 keys | 4,096 keys |
| --- | ---: | ---: |
| Durable puts (ops/s) | 260.050 | 260.034 |
| Compaction (ms) | 10.896 | 16.678 |
| Clean reopen before → after (ms) | 3.008 → 4.747 | 13.372 → 6.379 |
| SSTables before → after | 8 → 1 | 51 → 1 |
| Logical file bytes before → after | 415,649 → 124,525 | 3,258,462 → 930,156 |

Cached lookup p50 / p95 / p99, in µs:

| Dataset and stage | Existing | Missing |
| --- | ---: | ---: |
| 512, before | 0.583 / 0.667 / 1.250 | 0.958 / 1.125 / 1.250 |
| 512, after | 0.500 / 0.583 / 0.625 | 0.458 / 0.542 / 0.584 |
| 4,096, before | 0.750 / 1.042 / 1.166 | 2.292 / 2.625 / 2.833 |
| 4,096, after | 0.292 / 0.334 / 0.375 | 0.250 / 0.292 / 0.333 |

Raw reports include every repetition and lookup sample, exact commands, Python,
OS, hardware information, filesystem statistics, sync primitives, thresholds,
seed, timestamps, source hash, and operation/query/expected-state fingerprints:

- [512-key report](results/macos-arm64-python3.14.7-seed20261102-final.json)
- [4,096-key report](results/macos-arm64-python3.14.7-seed20261102-4096.json)
- [Earlier 512-key report](results/macos-arm64-python3.14.7-seed20261102.json)

For code comparisons, use fresh directories on the same filesystem with equal
parameters and workload fingerprints. Check environment, hardware, sync policy,
and methodology; disclose differences. A seed alone does not guarantee identical
workloads across generator/Python changes. Do not select or combine repetitions
from separate invocations to improve results. Source hashes may differ when code
changes; raw records remain unchanged.
