# Testing

Install the package and development dependency using the README instructions.
Run from the repository root:

```sh
.venv/bin/python -m pytest -q -ra --junitxml=.test-artifacts/junit.xml
.venv/bin/lsmkv --help
```

Runtime storage uses the standard library; tests require pytest. Selected suites:

```sh
.venv/bin/python -m pytest -q -m 'not realfs and not processkill'
.venv/bin/python -m pytest -q -m realfs
.venv/bin/python -m pytest -q -m processkill
LSMKV_KILL_SEEDS=11 .venv/bin/python -m pytest -q tests/test_process_kill.py -k compact
```

## Failure models

**SimFS:** separate inode-content and namespace durability, bounded crash-outcome
enumeration, byte-prefix partial writes, and I/O faults. Design sections 17–21
specify the enumerated/sampled cases and missing-barrier mutation checks. The
model does not cover every hardware/filesystem behavior.

**RealFS:** actual-directory initialization, exclusive creation, short/interrupted
writes, locks, close/reopen, flush, compaction, corruption rejection before
cleanup, descriptor lifetime, and interrupted tail repair. Mocked primitive
selection and unsupported-sync tests supplement native checks; mocking Linux
selection on macOS is not native Linux coverage.

**SIGKILL:** fresh child processes use started/run/completed/ACK handshakes with
at most one outstanding command. The parent records acknowledged mutations and
one optional in-flight mutation. Recovery compares the entire visible dictionary
against either allowed history, detecting unexpected keys too. Maintenance
commands do not change the dictionary history.

Default seeds 11 and 29 cover selected kills after a WAL write, after flush or
nonempty-compaction manifest rename, before empty-compaction input deletion,
after the final ACK, and during a free-running 128 KiB write. Two successive
recovery kills occur after tail truncation and before file sync, without a
successful recovery between them. A timeout test checks child reaping and lock
release.

Checkpoint kills record filesystem call number, method, and before/after phase.
The free-running write uses a seeded 0–2 ms delay; its exact syscall location is
scheduler-dependent and recorded as unknown. Seeds reproduce workloads and
requested delays, not kernel scheduling. Coverage is finite, not exhaustive.

**SIGKILL preserves the OS page cache.** It tests process interruption and
recovery, not power-loss durability. SimFS provides the separate modeled
power-loss checks; no test power-cycles hardware.

### Timeouts and reports

Children have a 90-second deadline, 10-second maximum protocol waits, and a
5-second kill/reap timeout. Tests clean up in finally blocks. Reports at
`.test-artifacts/sigkill-CASE-SEED.json` contain operations, acknowledgment status,
protocol events, seed, runtime, sync primitives, kill location, stderr, and exit
status. Workload reports also contain the recovered dictionary; recovery reports
preserve interrupted traces, with final state asserted by the test.

Set LSMKV_TEST_ARTIFACT_DIR to override the report directory. Reruns replace
reports with the same case/seed. pytest temporary directories also retain per-test
reports. Failed runs can have incomplete traces. Generated reports are ignored
by Git; benchmark measurement records under benchmarks/results are retained.

## Native durability and CI

RealFS requires Linux/macOS, flock, O_DIRECTORY/O_NOFOLLOW/O_CLOEXEC, and fsync.
macOS additionally requires fcntl.F_FULLFSYNC for file barriers. Directory and
parent-directory barriers use directory-descriptor fsync. Missing/rejected
required primitives raise DurabilityUnavailableError, without a weaker fallback.
Unsupported native filesystems fail rather than silently skipping. CLI info and
process reports record the selected primitives. OS/device behavior must honor
successful barriers; see Apple's [fsync/F_FULLFSYNC documentation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html).

[The CI workflow](.github/workflows/tests.yml) configures Ubuntu/macOS and Python
3.11/3.14, with full-suite execution, installed CLI help, and JUnit/trace artifacts.
Jobs have a 10-minute timeout. Native tests skip only on unsupported operating
systems. No GitHub Actions execution or successful CI run is recorded.

### Recorded validation

| Environment | Result | Evidence |
| --- | --- | --- |
| macOS Darwin 24.6.0 arm64, Python 3.14.7 | 282 passed, no skips | Full-suite command above; includes three benchmark tests and two targeted coverage regressions |
| Linux; distribution/Python/hardware unspecified | 277 passed, no skips | Reported test result; command and CI run URL unavailable |

Installed CLI put/get/flush/compact/info checks also passed. Linux test results
are not evidence of a GitHub Actions run. Benchmark raw records include their
own environments, commands, and state-verification results.

## CLI

Use `lsmkv --db DIRECTORY COMMAND` or `python -m lsmkv`. Commands are put KEY VALUE,
get KEY, delete KEY, flush, compact, and info. Keys/values are even-length hex
without spaces or a 0x prefix; quoted `''` means empty bytes. Output hex is
lowercase. The database directory is initialized if absent; its parent must exist.
Global --memtable-bytes and --wal-bytes options precede the command.

Successful commands emit JSON and exit 0. Missing get emits
`{"found": false, "value_hex": null}` and exits 1. Argument, storage, corruption,
and lock errors exit 2 with stderr diagnostics. info reports manifest metadata,
next sequence number, and sync primitives. Other successful mutations/maintenance
commands emit `{"ok": true}`. Storage errors fail an engine until close/reopen.
Use explicit close/context management for the Python API, and never remove or
replace LOCK while an owner is running.

Python API `put`/`delete` may raise after their WAL record is durable when a later
automatic flush fails. Such a put or deletion may appear after recovery; the
exception does not imply rollback. Close/reopen the failed engine before further
operations. See the [API contract](docs/design.md#2-api-and-limits).

## Targeted coverage regressions

Two coverage gaps were added without changing storage behavior. They are not
implementation defects and do not add entries to BUGS.md:

- `test_value_byte_corruption_is_rejected_without_structural_changes` encodes
  a valid single-entry table, confirms its successful decode, then flips only
  byte 35 (inside the value). Length, header, entry fields, key, and footer stay
  unchanged. The prior checksum test flipped byte 20, also invalidating the
  sequence field; it did not isolate value-checksum enforcement.
- `test_missing_manifest_with_only_synced_sstable_preserves_file` installs and
  syncs a valid SSTable and directory in SimFS, then crashes to confirm the
  fixture's durability. With no WAL or manifest, open must raise MissingManifestError.
  The test checks the exact original SSTable bytes, no MANIFEST or WAL creation,
  no unexpected names except LOCK, and preservation across another crash.

Baseline command (2 passed):

```sh
.venv/bin/python -m pytest -q \
  tests/test_sstable.py::test_value_byte_corruption_is_rejected_without_structural_changes \
  tests/test_engine.py::test_missing_manifest_with_only_synced_sstable_preserves_file
```

The complete mutation table for this check has exactly **two** entries. Each was
applied separately, tested, and restored in a finally block. No durability
barrier was removed. Source SHA-256 values before/after restoration match.

| Source and exact temporary mutation | Failing assertion and result | After restoration |
| --- | --- | --- |
| `src/lsmkv/sstable.py`: replace `if magic != END_MAGIC or zlib.crc32(data[:footer_offset]) != crc:` with `if magic != END_MAGIC:` | Value-corruption test: `with pytest.raises(CorruptionError, match='checksum'):` → `Failed: DID NOT RAISE CorruptionError`; 1 failed, exit 1 | 1 passed, exit 0 |
| `src/lsmkv/engine.py`, `_initialize`: remove the two-line `if names - allowed:` / `raise MissingManifestError('missing MANIFEST in an existing database')` guard | SSTable-only test: `with pytest.raises(MissingManifestError):` → `Failed: DID NOT RAISE MissingManifestError`; 1 failed, exit 1 | 1 passed, exit 0 |

The individual test commands used for both mutated and restored source were:

```sh
.venv/bin/python -m pytest -q tests/test_sstable.py::test_value_byte_corruption_is_rejected_without_structural_changes
.venv/bin/python -m pytest -q tests/test_engine.py::test_missing_manifest_with_only_synced_sstable_preserves_file
```

Execution records are saved under `.test-artifacts/coverage-regressions/`:
`mutations.json` records commands, exact replacements, exit codes, and source
hashes; `sstable-value-checksum-{mutated,restored}.txt` and
`missing-manifest-guard-{mutated,restored}.txt` contain pytest output. These local
generated files are ignored; the table above records the complete two-mutation
result. It is not a 24-mutation result or an aggregate claim about earlier checks.

After restoring both source files, the full-suite command at the top of this
document passed **282 tests in 9.36 seconds, no skips**. BUGS.md is unchanged:
these additions cover existing checks rather than fixing implementation defects.

## Benchmarks

[Benchmark documentation](benchmarks/README.md) contains exact commands, dataset
and query generation, timer boundaries, raw samples, and comparison rules. It
measures durable puts, cached lookup percentiles, clean reopen, and full
compaction, with whole-state/query verification outside timers. SSTables are
cached in memory; lookup measurements are not cold-disk reads. Reopen retains OS
caches. Performance thresholds are not part of the test suite.
