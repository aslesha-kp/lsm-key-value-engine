# lsmkv

A Python LSM key-value storage engine for Linux and macOS, with byte keys and
values, durable writes, and crash recovery. Runtime dependencies are limited to
the Python standard library.

## Features

- Checksummed write-ahead log, sequence recovery, and conservative torn-tail repair.
- Immutable sorted SSTables, byte-triggered memtable flushes, and WAL rotation.
- Atomic manifest installation and full-merge compaction with tombstone removal.
- Exclusive process locking, Linux fsync, and macOS F_FULLFSYNC.
- Deterministic simulated power-loss tests, I/O-error injection, and SIGKILL tests.

## Install and use

Requires Python 3.11+ and a filesystem supporting the required sync primitives.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

The CLI encodes byte keys and values as hexadecimal. This stores `key` → `value`
and reads it back; each command opens and closes the database:

```sh
.venv/bin/lsmkv --db example.db put 6b6579 76616c7565
.venv/bin/lsmkv --db example.db get 6b6579
# {"found": true, "value_hex": "76616c7565"}
```

The database directory is created if absent; its parent must exist. The CLI also
supports `delete`, `flush`, `compact`, and `info`. See [CLI details](TESTING.md#cli).

## Tests

```sh
.venv/bin/python -m pytest -q
```

See [TESTING.md](TESTING.md) for fault models, selected suites, and recorded results.

## Benchmarks

macOS 15.7.9 arm64, Python 3.14.7, 10 logical CPUs, 16 GiB RAM; per-write
F_FULLFSYNC. Seed 20261102, 8-byte keys, 256-byte values, three update rounds,
32 final WAL entries, 2,000 queries per category, and three repetitions.
Memtable/WAL thresholds: 64/256 KiB. Values are medians across repetitions;
arrows mean before → after full compaction.

| Measurement | 512 keys | 4,096 keys |
| --- | ---: | ---: |
| Durable puts (ops/s) | 260.050 | 260.034 |
| Cached existing lookup p99 (µs) | 1.250 → 0.625 | 1.166 → 0.375 |
| Cached missing lookup p99 (µs) | 1.250 → 0.584 | 2.833 → 0.333 |
| Clean reopen (ms) | 3.008 → 4.747 | 13.372 → 6.379 |
| Compaction (ms) | 10.896 | 16.678 |
| SSTables | 8 → 1 | 51 → 1 |
| Logical file bytes | 415,649 → 124,525 | 3,258,462 → 930,156 |

SSTables are cached in memory: lookups are not cold-disk reads. Reopen retains
OS caches and is not a crash-recovery measurement. These small runs do not
establish general performance scaling. [Methodology, reproduction commands,
percentiles, and raw results](benchmarks/README.md).

## Limitations

Single owner and serial operations; no Windows support. Keys are limited to
64 KiB and values to 16 MiB; empty bytes are valid. SSTables, WAL replay, and
compaction use in-memory buffers. Compaction merges all installed tables.
No sparse indexes, Bloom filters, group commit, or replication. SIGKILL tests
leave OS caches intact; power-loss checks use a bounded simulated model.

[Design and durability contract](docs/design.md) · [Testing](TESTING.md) ·
[Benchmarks](benchmarks/README.md) · [Defects and regressions](BUGS.md)
