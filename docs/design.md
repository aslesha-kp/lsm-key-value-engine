# LSM Key-Value Storage Engine: Design Document

Design v0.3 with implementation refinements. Sections 1–16 define the storage
contract; sections 17–23 record implementation decisions and validation. Earlier
milestone deferrals are historical and are superseded by later sections.

## 1. Goal and scope

The engine answers one question: **when the system says a write succeeded, can that value be recovered after a crash?**

In scope for v1: sequence-numbered writes and deletes, checksummed WAL, in-memory memtable, immutable SSTables, durable manifest, full-merge compaction, reference-model tests, injected-crash tests.

Out of scope for v1: replication, Raft, sharding, transactions, network service, multi-process access, concurrency (operations run serially), group commit, sparse index, Bloom filters, range scans. Optional RealFS measurement tooling is described in section 23.

## 2. API and limits

- `put(key: bytes, value: bytes) -> None`
- `get(key: bytes) -> bytes | None`
- `delete(key: bytes) -> None`
- Limits: key up to 64 KiB, value up to 16 MiB. Empty values are allowed. Larger inputs raise an error before anything is written.

`put` and `delete` can raise a storage error **after their WAL record became
durable** if a subsequent threshold-triggered flush fails. An exception does not
imply rollback: that put or deletion may appear after recovery even though the
call did not return success. The engine enters the failed state; close and reopen
it before further operations. Input-validation errors still occur before writing.

## 3. Durability contract

A call to `put` or `delete` returns success only after its WAL record has been written and synced. A returned success means the write survives a crash under the assumptions in section 4.

| Mode | When success is returned | Guarantee |
| --- | --- | --- |
| `always` (default) | After this write's WAL record is synced and any triggered flush completes | Acknowledged writes survive a crash under the stated assumptions |
| group commit (deferred) | After the group containing this write is synced | Same acknowledgment guarantee, different cost; not in v1 |
| `unsynced` | Before any sync | Recent acknowledged writes may be lost; used only for benchmarking |

The operation in flight at the moment of a crash may appear after recovery fully or not at all, never partially. Operations that were never started never appear.

**Two separate promises.** Crash durability: under the failure model in section 4, every acknowledged write survives a crash. Corruption detection: if stored bytes are damaged, the engine reports the damage within the limits of its checksums. A 32-bit CRC can miss some corruption, so detection is not absolute, and it never substitutes for durability.

## 4. Assumptions and failure model

- One process owns the directory, enforced with a lock file. A second opener fails.
- Target platforms: Linux and macOS. Windows is unsupported.
- File sync: `fcntl.F_FULLFSYNC` on macOS, `os.fsync` elsewhere. Directory sync: `os.fsync` on a directory file descriptor. The implementation records which primitive it used, and benchmark reports state it.
- Crash model: a crash may lose data that was written but not synced, or persist only a prefix of it, torn at any byte. Unsynced directory operations (create, rename, unlink) may or may not persist.
- Corruption model: checksums detect most accidental corruption, within the limits of a 32-bit CRC; some damage can go undetected. The engine reports detected damage and does not repair it. This is a separate promise from crash durability.
- **Not covered:** drives or controllers that misreport sync completion, filesystem bugs, and hardware failure modes beyond what the sync call promises. Tests establish correctness only within the modeled failures.
- **Sync failure is fatal.** After any write or sync error, the engine marks itself failed and rejects all further operations until the directory is reopened and recovered. It never retries a failed sync.

## 5. Sequence numbers and versions

Every put or delete gets a `seq` (u64) from a counter that starts at 1, strictly increases during execution, and is never reused for an acknowledged write. A number assigned to an operation whose record was lost before it was synced was never acknowledged, and may be reused after recovery. Valid WAL records have positive, strictly increasing sequence numbers across files; gaps are allowed, duplicates and decreases are corruption. When two versions of a key exist, the higher `seq` wins. After recovery, `next_seq = max(highest seq in valid WAL records, flushed_seq, highest max_seq among manifest SSTables) + 1`.

## 6. On-disk layout

```
db/
  LOCK
  MANIFEST
  wal-000001.log
  sst-000001.sst
  *.tmp            (transient files; deleted during recovery)
```

### WAL file

Header: 8-byte magic `LSMWAL01`, u32 LE version 1. Then records:

```
len          u32 LE   payload length
header_crc   u32 LE   CRC32 over the 4 len bytes only
payload_crc  u32 LE   CRC32 over the payload bytes only
payload:
  seq   u64
  op    u8      1 = put, 2 = delete
  klen  u32
  key   bytes
  vlen  u32     0 for delete
  value bytes
```

The 12-byte record header (`len`, `header_crc`, `payload_crc`) is verified before the payload is read. Recovery trusts `len` only after `header_crc` matches, so a corrupted length is reported as corruption instead of being mistaken for an incomplete record.

### SSTable file (format version 1)

```
header: magic(8, LSMSST01), version u32 LE (1)
entries, sorted by key, one entry per key:
  klen u32, vlen u32, seq u64, op u8, key, value
footer (fixed size, written last):
  entry_count u64, min_seq u64, max_seq u64,
  data_crc32 u32 (over all bytes before the footer), magic(8, LSMEND01)
```

A file without a valid footer is incomplete. In v1, lookups load the file into memory. A sparse index is a later format version.

### Manifest

```
magic(8, LSMMAN01) | version u32 LE (1) | payload_len u32 LE | crc32 u32 LE | payload (UTF-8 JSON)
payload: { generation, flushed_seq, next_file_number,
           sstables: [ {name, size, min_seq, max_seq}, ... ] }
```

`flushed_seq` means: every write with `seq <= flushed_seq` has its effects represented by the installed SSTable state, so its WAL records are not needed for recovery. This stays true after overwrites and after compaction drops delete markers.

## 7. Write path and read path

**Write:** (1) refuse if the engine is in the failed state; (2) assign `seq`; (3) append the WAL record and sync; (4) apply to the memtable; (5) if the memtable or the active WAL has reached its size threshold, run a flush; (6) return success. The memtable is updated only after the WAL sync, so a failed WAL write never becomes visible to reads.

**Read:** check the memtable, then SSTables from newest to oldest. The first entry found decides the result. A delete marker means the key is absent. Invariant: SSTables cover disjoint, increasing `seq` ranges, so newest-file-first is equivalent to highest-`seq`-first.

The memtable is a `dict` of key to `(seq, op, value)`, sorted only at flush.

## 8. Flush and installation order

The order below is the core invariant. Each step must be durable before the next begins.

1. Freeze the memtable. Create a new WAL file (sync the file and the directory) and direct new writes to it.
2. Write the new SSTable to a `.tmp` file, sync it, rename it to its final name, then **sync the directory** so the entry is durable.
3. Write the new manifest to `MANIFEST.tmp` with the SSTable added and `flushed_seq` set to the highest `seq` in the frozen memtable. Sync it, rename it over `MANIFEST`, then **sync the directory**.
4. Only now delete non-active WAL files (never the active WAL, which is empty right after rotation) whose records all have `seq <= flushed_seq`.

Crash analysis:

- Before step 3 completes: the manifest is unchanged. The new SSTable is an orphan and is deleted at recovery. The old WALs still hold every write.
- After step 3, before step 4: the manifest covers the writes. Old WALs are redundant, and recovery skips their records by watermark.

A flush of an empty memtable does nothing: no SSTable is written, the manifest is unchanged, and the WAL is not rotated.

**Flush triggers.** A flush starts when the memtable reaches 4 MiB of encoded live entries or the active WAL reaches 16 MiB; both are configurable. The WAL size is a trigger, not a hard limit: a valid record may push the file past it before the flush runs. Without the WAL trigger, repeatedly overwriting one key would keep the memtable small while the log grew without bound.

## 9. Recovery algorithm

1. Acquire the lock. The engine's own `LOCK` file is ignored when deciding whether the directory is new.
2. **Directory state and initialization.** Initialization order: create `wal-000001.log` with a complete header and sync the file and the directory; only then write, sync, and install the initial manifest (generation 0, no SSTables, `flushed_seq` 0) and sync the directory. A manifest therefore never exists without a WAL. At open: (a) if the directory does not exist or contains nothing except `LOCK` and/or `MANIFEST.tmp`, initialize a new database; (b) if `MANIFEST` is missing and the directory contains nothing else except `wal-000001.log` and optionally `MANIFEST.tmp` (with `LOCK` ignored), and that WAL file is either shorter than the WAL header or consists of a validated header (correct magic and version) and nothing more, treat it as interrupted initialization: for a shorter file, recreate the header and sync the file and the directory, then install the manifest. The temporary manifest contents are ignored and replaced during this initialization retry. No write can have been acknowledged before the manifest exists, so nothing is lost; (c) in every other case with no `MANIFEST`, **fail with a clear `MissingManifestError`**; never treat it as a new database and never delete anything; (d) if `MANIFEST` exists but no WAL file exists, fail with a fatal error at any generation, because the manifest is installed only after the first WAL and every flush leaves a WAL, so its absence means something was deleted.
3. **Validate the manifest** (magic, version, length, checksum). A corrupt manifest is a fatal error.
4. **Validate every referenced SSTable** in full: the file exists and its size matches the manifest; header and footer magic and version are valid; the data checksum over all bytes before the footer matches; the entries parse with exact record boundaries ending at the footer; keys are strictly ascending; the entry count matches the footer; and `min_seq` and `max_seq` agree among the entries, the footer, and the manifest. Any failure is fatal. This reads all SSTable data at open, which is acceptable in v1.
5. **Clean up** only after steps 3 and 4 and a validation-only pass over all WALs succeed (M2 refinement): delete `*.tmp` files and `sst-*` files not listed in the manifest.
6. **Replay WALs** in file order, validating each file's header first. Skip records with `seq <= flushed_seq`. Apply the rest to the memtable.
7. **Torn-tail rule (conservative):** `header_crc` is checked before `len` is trusted. Only an *incomplete* trailing record in the *last* WAL file is tolerated: fewer than 12 header bytes remain, or a header with a valid `header_crc` whose payload is cut short by end of file. Truncate the file to the last valid record boundary and sync. Everything else is a `CorruptionError`: a `header_crc` mismatch (including a zero-filled tail), a `payload_crc` mismatch on a complete record (even at the end), any invalid or incomplete record in a non-last WAL file, a record length above the legal maximum, or a zero-length record. There is no option to tolerate checksum failures in v1.
8. **WAL file header:** a last WAL file shorter than its header is treated as freshly created and empty. Before any append, recreate it with a valid header and sync the file and the directory. A complete header with a bad magic or version is a `CorruptionError`.
9. Set `next_seq` per section 5. Delete WAL files fully covered by `flushed_seq`, except the last WAL file, which is the active WAL and is never deleted.

**Watermark invariant:** records with `seq <= flushed_seq` are never replayed into the memtable, and compaction runs only after recovery completes. This matters because replaying an old put into the memtable after its delete marker has been dropped by compaction would resurrect the key. Version resolution by `seq` makes replay harmless only while that invariant holds, so it is tested directly.

## 10. Compaction (full merge only)

1. Merge all SSTables by key ascending, `seq` descending. Keep only the newest version of each key.
2. **Drop delete markers.** This is safe only because the merge covers every SSTable, so no older version of the key exists in any file. Entries in the memtable are always newer than any SSTable entry.
3. Install using the same order as a flush: write, sync, and rename the output, sync the directory; write, sync, and rename the manifest, sync the directory; then delete the input files. `flushed_seq` does not change.

A crash before the manifest install leaves the output as an orphan. A crash after it leaves the inputs as orphans. Recovery removes either.

**Empty output.** If the merge leaves no entries because every key was deleted, no SSTable file is written. The new manifest lists zero SSTables and keeps `flushed_seq` and `next_file_number` unchanged, so no sequence number of an acknowledged write is reused after reopen. A database with zero SSTables is valid at recovery, and the input files are deleted after the manifest install as usual.

## 11. Testing strategy

**Filesystem abstraction.** All I/O goes through a thin interface (open, write, sync, rename, unlink, sync\_dir). `SimFS` tracks durable and pending state separately. At a crash it discards unsynced data, keeps an arbitrary prefix of pending writes (torn at any byte), and applies or drops pending directory operations in any order.

**Crash injection.** Run a workload and crash at every FS call (exhaustive for small workloads, seeded random for large ones). After recovery, the database state must equal the reference model after applying the acknowledged operation history in order, or that history plus the single in-flight operation. An acknowledged write may legitimately be absent when a later acknowledged operation overwrote or deleted it, so the oracle compares whole states, not individual writes.

**Crashes during recovery.** Recovery truncates files, recreates headers, installs the initial manifest, and deletes obsolete files, so it is crash-tested too. Inject a crash at every filesystem call that recovery makes, reopen, and verify the same state guarantees; then repeat with a second crash during that second recovery. Recovery's own unsynced truncations and deletions may be lost at the crash, and the result must be correct either way.

**Real process tests.** A child process runs the engine and reports each acknowledgment to the parent; the parent sends SIGKILL at random times, reopens the directory, and verifies the state. This catches real-OS behavior and bugs in the simulation. It does **not** test missing syncs, since the OS cache survives a process kill; that is the job of `SimFS`.

**Reference model.** Randomized put/get/delete sequences compared with a Python dict, including after reopening.

**Targeted tests:** torn tail truncated at every byte offset of the last record; checksum flip in the middle of a log; checksum flip in the last complete record (must error); zero-filled tail; missing manifest; corrupt manifest; missing referenced SSTable; orphan SSTable and `.tmp` cleanup only after validation; interrupted flush at every crash point; interrupted compaction; repeated compactions; delete then flush then compact then reopen; watermark behavior (records at or below `flushed_seq` are not replayed; replaying them with the watermark absent still resolves correctly while no delete marker has been dropped); `next_seq` monotonic across reopen; second opener rejected by the lock.

**Tests added from review:** `header_crc` corruption in the length field of the first, a middle, and the last record; a zero-filled tail reported as corruption; a last WAL file shorter than its header is recreated with a synced header and accepts appends; a complete WAL header with bad magic or version errors; a directory containing only `LOCK` opens as new; interrupted initialization recovers (see the v0.3 tests below), and a missing WAL fails at any generation; an SSTable with a bad data checksum, out-of-order keys, a truncated entry, or `min_seq`/`max_seq` disagreeing with the manifest fails at open with nothing deleted; compaction after every key was deleted leaves zero SSTables, reopens correctly, and never reuses a sequence number.

**Tests added in v0.3:** crash at every filesystem call during recovery, reopen, then crash again during the second recovery, verifying the state guarantees each time; a manifest-less directory holding only a validated header-only `wal-000001.log` initializes; the same with a shorter first WAL, which is recreated and synced before the manifest is installed; a manifest-less directory whose first WAL has an invalid header or contains any record raises `MissingManifestError` and deletes nothing; a database with acknowledged writes, still at generation 0, whose WAL is removed fails at open instead of creating an empty WAL; repeatedly overwriting one key triggers a flush by WAL size, and the log exceeds the threshold by at most the record that crossed it.

## 12. Milestones

0. Test harness: FS interface, `SimFS`, reference model, randomized operation generator.
1. WAL, checksums, replay, torn-tail handling, `put`/`get`/`delete` over a `dict` memtable.
2. SSTables, manifest, flush, full recovery algorithm, crash tests for flush.
3. Full-merge compaction, delete-marker handling, crash tests for compaction.
4. Real-process kill tests, CLI, CI, README, final write-up.

After milestone 4: sparse index, Bloom filters, range scans, benchmarks, group commit, and possibly an opt-in tail-corruption policy.

## 13. Known limitations

- A 32-bit CRC does not guarantee detection of every corruption. `header_crc` makes a corrupted length field detectable instead of looking like a torn record, but some damage can still go undetected.
- A filesystem that extends a file with zeros before persisting its data leaves a zero-filled tail, which fails `header_crc` and is reported as corruption rather than tolerated. An opt-in tail policy, if ever added, would address this.
- No protection against hardware failure beyond what the sync primitive provides.
- Single process, serial operations, whole-file SSTable loads, and full SSTable verification at open, whose cost grows with data size, in v1.

## 14. Decisions (resolved)

- **Memtable flush threshold:** 4 MiB of encoded live entries, configurable. One oversized entry is allowed into the memtable, then the memtable flushes.
- **WAL flush trigger:** the engine also flushes when the active WAL reaches a configurable size, 16 MiB by default. It is a trigger, not a hard limit: one valid record may push the file past it before the flush runs.
- **WAL rotation:** only at memtable flush in v1. Independent size-based rotation is deferred.
- **Checksum:** `zlib.crc32`. Coverage: `header_crc` over the 4 `len` bytes; `payload_crc` over the payload; SSTable `data_crc32` over all bytes before the footer; manifest `crc32` over `version`, `payload_len`, and the payload.

## 15. Changelog (v0.1 to v0.2)

| Section | Change |
| --- | --- |
| 3, 4 | Separated crash durability from corruption detection, and stated that a 32-bit CRC cannot guarantee detection. |
| 6, `flushed_seq` | Redefined: the effects of all writes through this sequence are represented by the installed SSTable state, so those WAL records are unnecessary for recovery. |
| 6, WAL record | Split the checksum into `header_crc` over the length and `payload_crc` over the payload. |
| 8 | A flush of an empty memtable does nothing. |
| 9, initialization | The `LOCK` file is ignored when checking whether a directory is new; added rules for interrupted initialization. |
| 9, SSTable validation | Open now verifies the data checksum, record boundaries, key order, entry count, and metadata consistency before any cleanup. |
| 9, WAL recovery | Torn-tail rule uses `header_crc`; a short WAL header is recreated and synced before appending; a complete invalid header is an error. |
| 10 | Defined empty compaction output: zero SSTables, `flushed_seq` preserved. |
| 11 | Crash-test oracle compares against the acknowledged operation history; added targeted tests. |
| 13 | Rewrote known limitations now that the corrupted-length gap is narrowed. |
| 14 | Resolved the open decisions: 4 MiB threshold, WAL rotation at flush only, `zlib.crc32` with documented coverage. |

## 16. Changelog (v0.2 to v0.3)

| Section | Change |
| --- | --- |
| 5, 10 | Sequence numbers are never reused for acknowledged writes; an unsynced, unacknowledged assignment may be reused after recovery. |
| 7, 8, 14 | A flush also triggers when the active WAL reaches a configurable size (default 16 MiB). It is a trigger, not a hard limit. |
| 8 | Cleanup after a flush never deletes the active WAL. |
| 9 | Initialization creates and syncs the first WAL before installing the manifest. Interrupted initialization is recognized only for a manifest-less directory holding exactly a validated header-only or shorter `wal-000001.log`. A manifest with no WAL is fatal at any generation. |
| 11 | Added crash-during-recovery tests, initialization tests, and a WAL-size trigger test. |

## 17. M0 implementation decisions and refinement record

M0 uses Python 3.11+, the `lsmkv` package in a `src` layout, and pytest.
Runtime code uses only the standard library. The storage boundary is the
`FileSystem` protocol: create, explicit-offset read/write (default write is
append), truncate, listdir, sync_file, sync_dir, atomic replacement rename,
unlink, and opaque lock/unlock. The boundary represents a single database
directory with an already durable parent; all file names are single components.
The directory itself may be absent (see the M0 harness extension below).
No host storage I/O is performed by SimFS. A real OS adapter, including the
platform sync primitives in section 4, is deferred; M0 does not claim to test
those primitives. SimFS records its simulated primitive in `sync_primitive`.

### Precise simulated persistence choices

- File identities (inodes), live names, durable names, and pending namespace
  edits are separate. New inodes start with durable empty contents. A file
  sync commits that inode's current bytes and clears its pending content
  changes, without committing any filename. A directory sync commits the
  live namespace and clears pending namespace edits, without syncing bytes.
- Writes produce ordered byte microsteps, including overwrites. Sparse writes
  zero-fill the gap at the first written byte. Truncation is one metadata
  microstep (old or new length); extensions zero-fill. Each inode's selected
  unsynced outcome is a prefix of its microstep history since its last file
  sync. This defines combined write/truncate ordering and allows every byte
  boundary in an unsynced append, without undoing completed file syncs.
- Pending directory edits are captured assignments/removals of inode identities.
  A crash selects any ordered subset of those edits over the last synced
  namespace. Each rename edit removes the source and assigns the destination
  atomically, including replacement. Reordering edits can yield multiple names
  for an inode or resurrect a deleted name; these are conservative adversarial
  states allowed by section 11's arbitrary-order rule, not a claim that every
  such state occurs on a particular real filesystem. Directory barriers cut
  off that reordering: committed edits cannot subsequently be dropped.
- `CrashChoice` explicitly selects content prefix counts and directory edit
  indices. Unspecified prefixes and directory edits are dropped by default.
  Live/durable names identify inodes where unambiguous; reserved `@inode:N`
  identifiers retain overwritten identities. User filenames with that prefix
  are rejected. Choices are local to the exact pre-crash state.
- A crash makes the selected image the next durable baseline, drops remaining
  pending state, and releases volatile locks. Thus a subsequent crash cannot
  undo the previous crash's surviving image. Locking models exclusive ownership
  within this SimFS instance, independent of persistent lock-file names; it
  neither creates nor syncs a LOCK entry.

`enumerate_crashes()` enumerates the Cartesian product of every reachable
inode's microstep prefix and every ordered subset of pending directory edits.
It is exhaustive only for this finite modeled state, costs factorial time in
directory edits, and may yield duplicate images. Tests enumerate all five
ordered subsets of two edits, all five byte prefixes of a four-byte append,
and all ten choices in an overwritten-inode case. Larger crash samples use
`sample_crash(seed)`; the current reproducibility test uses seeds 0–19, not
exhaustive coverage. Workload generation uses a private seeded Random and
small keys/values, with reproducibility within a Python runtime; callers can
record the generated operations and CrashChoice for cross-version replay.

The history oracle compares whole dictionary states: the acknowledged state,
or that state with exactly one in-flight operation applied. Reads do not
mutate the dictionary. Accepting a recovery image establishes a new baseline
and clears the in-flight operation. This permits later acknowledged overwrites
and deletes to supersede earlier values and rejects partially applied puts.

### Limits and remaining work

M0 tests filesystem crash behavior and the oracle, not database recovery.
Repeated crash tests exercise recovery-like truncation and namespace changes;
the section 11 engine recovery algorithm remains for later milestones.
SimFS has no exposed file handles, general directory tree, permissions, host
processes, concurrent access, arbitrary bit corruption, or device/controller
model. Pending content uses compact mutation records, and the harness injects
call-boundary crashes and I/O errors as specified below. Exhaustive crash
enumeration still requires tiny workloads. Unsynced writes are modeled by ordered prefixes, not arbitrary
sector subsets or zero-filled device tears. Unsynced truncations are atomic
old/new choices. These are explicit refinements of the prefix failure model,
not evidence of real filesystem behavior. Sync errors and fatal engine state
remain later-milestone responsibilities; section 4's guarantees are unchanged.

Change record: added this section during M0 to specify packaging, the I/O
boundary, inode/namespace separation, deterministic enumeration and sampling,
truncation ordering, lock semantics, and simulation limits. No on-disk format
or acknowledged-write guarantee was changed.

M0 review refinement: rename between two names already referring to the same
inode is a no-op, matching POSIX. This matters because the conservative
arbitrary-order crash model can produce aliases. Isolated renames, including
replacement of an existing destination, have exactly two namespace outcomes:
the complete pre-rename state or the complete post-rename state. Compositions
of multiple unsynced edits retain the arbitrary-order semantics above; they
are not constrained to a prefix of the live namespace operation history.

The initial M0 review identified missing directory provisioning and lifecycle
operations. The extension below resolves that interface gap. The real adapter
remains deferred: it must own the correct file descriptors for sync, loop over
short writes, propagate I/O errors, enforce cross-process locking, and use the
platform primitives in section 4. None of those OS behaviors is validated by M0.

## 18. M0 harness extension (before M1)

### Compact content log

An inode stores durable bytes plus one immutable record per write (offset and
payload) or truncation (new size). A write record still represents one modeled
microstep per byte; a truncation still represents one microstep. CrashChoice
prefix counts retain exactly their previous meaning. Reconstruction applies
complete records then, if necessary, a byte prefix of the next write, including
sparse zero filling only when at least one byte survives. No intermediate
full-file snapshots are retained. Memory is linear in durable bytes, pending
payload bytes, mutation count, and the largest materialized image. Reads,
file sync, and crash reconstruction replay records; repeated reads/appends can
still incur replay cost. This is not a performance-optimized production FS.

Tests retain exhaustive tiny byte boundaries, compare mixed mutations against
an independent tiny snapshot reference for seeds 0–19, and exercise a 2 MiB
write/read/partial crash with a 24 MiB tracemalloc peak ceiling. The latter is
a bounded memory regression, not exhaustive large-write crash coverage.

### Provisioning and lifecycle

`directory_exists`, `ensure_directory`, `sync_parent`, `close`, and `reopen`
are added to FileSystem. The bound path's parent is assumed durably provisioned;
arbitrary recursive directory creation is outside this single-directory model.
`ensure_directory` creates the database directory idempotently, without syncing.
`sync_parent` persists its entry independently of file and child-directory syncs.
A new unsynced root may survive or disappear at crash, selected explicitly by
CrashChoice.directory_survives (None defaults to the last parent-synced state).
Enumeration includes both choices; seeded sampling records its chosen boolean.
If the directory disappears, its child namespace and inodes are inaccessible
and discarded. After parent sync it cannot disappear under this model. Existing
SimFS() instances start with a durably existing directory for compatibility;
SimFS(directory_exists=False) tests provisioning.

`close` releases locks and makes normal I/O unavailable; it does not sync or
crash. It is idempotent. `reopen` re-enables I/O without initialization, recovery,
or implicit persistence; pending data still exists until a simulated crash.
Crash releases locks and resets the simulated session to open. Stale tokens
cannot unlock later locks. SimFS remains one ownership domain; real independent
process/session behavior remains the OS adapter's responsibility.

### Automatic crashes and explicit I/O errors

`FaultFS` wraps all FileSystem calls, records zero-based call indices and
before/after events, and delegates normal I/O through the interface. A one-shot
FaultPlan selects a call, phase, and crash or EIO error. Before-call errors have
no effects except an explicitly selected partial write prefix; after-call errors
retain the full call's effects. Sync errors can therefore have no effect or have
completed persistence before error delivery. This samples relevant error cases;
it does not enumerate every device error or internal syscall interleaving.
The injector never retries or marks an engine failed; testing that a future
engine rejects operations after write/sync errors is M1 work. InjectedCrash is
outside Exception handlers, stops the workload before further acknowledgments,
and leaves persistence selection to the runner (it does not itself call crash).
The wrapper latches a crash and rejects all later I/O, including finally-block
cleanup. Workload code must route I/O through the supplied wrapper and must not
catch InjectedCrash or acknowledge operations while unwinding a crash.

`run_crashes(factory)` discovers a deterministic successful workload's call
trace, recreates a fresh case at every before/after event, and stops at that
boundary. It also checks workload completion, after the final acknowledgment.
At each boundary it clones the image for every enumerated CrashChoice, crashes,
reopens, invokes the supplied recovery callback through FaultFS, and passes the
result and event to the supplied check callback. The case factory must create
fresh history and state; recovery/check callbacks must not mutate that shared
history across choices. Assertions/errors propagate. A changed call trace is
rejected, but argument/history determinism is the caller's responsibility.

With `seeds=(...)` it samples one recorded choice per seed per boundary instead
of enumerating. Reports retain boundary, choice, and seed. Recovery can itself
be supplied as the workload to enumerate a crash during recovery followed by a
second recovery; a regression exercises unsynced truncation this way. This
covers the calls of each supplied workload, not all possible workloads or
crashes within a completed barrier. Tiny scenarios enumerate full modeled
persistence products; larger scenarios must use explicit sampled seeds.

The positive installation test syncs both data and filename, adds unrelated
pending writes/rename, and verifies the same acknowledged-history oracle for
all 15 modeled outcomes. Negative missing-barrier tests remain passing checks
that assert oracle rejection. No WAL, SSTable, manifest, or engine was added.

Change record: replaced quadratic byte snapshots while preserving microstep
outcomes; added provisioning/lifecycle, automatic boundary crash enumeration,
completion checks, explicit EIO injection, and positive durability-oracle tests.
The original durability promises and conservative directory ordering are unchanged.


## 19. WAL implementation and bootstrap refinement

### Final-format bootstrap dependency

M1 implements the WAL-backed API plus only the manifest subset necessary for
compatible initialization. On-disk version 1 is now fixed: WAL magic LSMWAL01
and manifest magic LSMMAN01, with the little-endian fields specified above.
There is no temporary M1 database format or migration requirement. The initial
manifest uses generation 0, flushed_seq 0, next_file_number 2 (WAL 1 is already
allocated), and an empty SSTable list. Its checksum covers version, payload_len,
and payload as specified in section 14. JSON writing uses sorted keys and compact
separators; validated UTF-8 JSON does not have to use that serialization order.
Duplicate keys, malformed fields, booleans in integer fields, bad checksums,
wrong lengths, and trailing bytes are rejected.

The bootstrap implementation writes, validates, and installs
the initial manifest using the final format and §9 ordering; no general manifest
updates, SSTable loading, flushing, or compaction. Valid nonzero generations,
nonzero flushed_seq, or nonempty SSTable lists raise UnsupportedFormatError.
Unknown directory files with a manifest likewise raise UnsupportedFormatError;
M1 does not perform general orphan/tmp cleanup. It may leave MANIFEST.tmp next
to a valid manifest; M2 must perform the validation-before-cleanup steps in §9
before ever reusing that temporary pathname (it may be an alias in SimFS).

Section 9(b) permits an interrupted initialization state: MANIFEST.tmp may coexist with only a
short or validated header-only wal-000001.log during interrupted initialization.
This is a reachable crash state while writing the temporary initial manifest.
WALs with any bytes beyond the complete header, or invalid complete headers,
still raise MissingManifestError when MANIFEST is absent, including when a
temporary manifest exists. Nothing is deleted on these failures. The primary
rule in §9 has been updated, rather than leaving conflicting requirements.

### API, recovery, and failure behavior

`Engine.open(fs)` reopens the supplied adapter, provisions the bound directory,
and syncs its parent before acquiring exclusive LOCK ownership. It creates
LOCK when the adapter's lock operation did not already do so. An unsuccessful
open releases only its acquired lock; an unsuccessful second opener does not
close the first owner's adapter. A successful Engine owns that adapter until
close. `close()` releases the lock and closes the adapter without additional
syncs; it is idempotent. After a failed write/sync, callers must close the
failed Engine and open a new Engine to recover. No operation is retried by the
engine. Cleanup close is permitted; put/get/delete reject the failed state.

`put` and `delete` encode and validate all inputs before assigning a sequence
or issuing storage writes, then append and sync the active WAL. Only after
successful sync do they change the dict memtable, which stores
key -> (seq, op, value). Empty keys and values are supported; get returns None
for missing/deleted keys. Limits are exactly 64 KiB keys and 16 MiB values.
An exhausted u64 sequence space raises OverflowError before storage mutation;
this is not an I/O failure and does not mark the engine failed. Only always-sync
writes are supported in M1; the benchmark unsynced mode remains deferred.

Replay is side-effect-free until a WAL is validated. It checks the length
checksum before trusting the length, rejects lengths outside 17..MAX_PAYLOAD
(the minimum payload contains seq, op, klen and vlen), and validates complete
payload checksums and exact internal boundaries. Delete payloads must have
vlen 0. Sequence numbers must be positive and strictly increasing across all
replayed WALs; gaps are allowed. Replays return record boundaries to the
engine; only an incomplete final record in the last WAL can be truncated.
An invalid complete record, including a checksum failure at EOF or a zero-filled
12-byte tail, is corruption and is never silently discarded. A short non-last
WAL header is corruption; a short last header is recreated and synced as §9
requires. Existing WALs are replayed in canonical filename order; M1 never
rotates or deletes WALs. next_seq is the highest valid sequence plus one.

Successful open also syncs all replayed WAL files and the directory before
exposing recovered values. This establishes a durable recovery baseline even
when reopening without a simulated crash after a failed append/sync: visible
complete unacknowledged bytes can still be pending in that case. Recovered
complete operations may be included or excluded according to what survived;
partial operations never become visible. The additional barriers strengthen,
rather than replace, the append acknowledgment barrier. Sync failures during
open abort the open; no usable Engine is returned.

### M1 validation and limits

The M0 runner is used for all before/after filesystem-call boundaries plus
workload completion. Tiny write workloads enumerate every byte-prefix crash
choice; larger workloads sample seeds explicitly. Five 30-operation reference
workloads use seeds 0, 1, 7, 42 and 2026, with crash samples seed and seed+1000.
Tests include overwrite/delete/empty values, exact API limits, malformed record
structure, positive/global sequence ordering, header/payload checksums in first,
middle and last records, and every cut in three small final records. Final
record examples have 29, 31, and 30 bytes respectively: all 90 incomplete cut
positions are checked (including the empty tail boundary), plus complete cases.

Initialization plus the first acknowledged put exercises 661 enumerated crash
cases. Recovery exercises 29 first/769 second crashes for torn-tail repair,
59 first/2977 second crashes for short-header repair, and 227 first/46713 second
crashes for initial-manifest completion. These counts include duplicate images
and are exhaustive for each supplied finite scenario and modeled call trace,
not for all possible workloads or hardware behaviors. Recovery and initialization
write/sync errors are injected before and after each relevant call; append
partial-write errors and initial header/manifest partial-write errors cover
every byte boundary of small inputs. No retry is allowed after a failing barrier.

The required mutation check temporarily removed only the active-WAL append sync.
`test_acknowledged_wal_append_survives_every_crash_choice` then failed because
recovery returned {} instead of {b'k': b'acknowledged'}. The original source was
restored in a finally block and the same test passed. This is deliberately
injected validation evidence, not a newly discovered production defect.
Reinstating the original stricter initialization rule also failed the
MANIFEST.tmp exception regression; that original design defect is in BUGS.md.

M1 has no real OS adapter, process-kill tests, general manifest recovery,
SSTables, flushing, WAL-size/memtable flush triggers, compaction, CLI, or CI.
WALs grow without bound until M2; replay currently reads each entire WAL into
memory. The M0 prefix/namespace failure model and its hardware limitations
remain unchanged. M1 tests correctness only under that model and injected EIO
cases, not actual platform sync semantics or every possible I/O failure.

Change record: fixed the interrupted-initialization rule in §9,
selected stable format magic/version constants, clarified legal payload lengths
and sequence ordering, implemented final-format bootstrap metadata, and added
recovery-baseline barriers and M1 validation evidence. The acknowledged-write
durability contract was not weakened.


## 20. M2 implementation decisions and validation

M2 implements full version-1 manifest metadata, immutable sorted tables,
threshold-triggered and explicit flush, WAL rotation/retirement, and recovery.
It does not implement compaction, Bloom filters, sparse indexes, group commit,
or a real OS adapter. Section 19 records the historical M1 subset restrictions;
the M2 engine now uses full manifest validation. `validate_m1` remains a legacy
subset validator so the original M1 format tests continue unchanged.

### Allocation, ordering, and immutability

One positive u64 file-number domain is shared by WALs and SSTables. Initialization
allocates WAL 1 and records next_file_number 2. A normal first flush allocates
WAL 2 and SSTable 3, then records next_file_number 4. Filenames use canonical
decimal integers padded to a minimum of six digits; numeric order, not lexical
order, determines WAL replay and active-WAL selection across digit widths.

Before cleanup, recovery sets the in-memory allocator to the maximum of the
manifest's next_file_number and one plus every observed canonical numbered WAL,
SSTable, or numbered temporary file. It never reuses an observed number in the
same session, including after deleting an orphan. Completed manifest installs
persist the advanced counter. If an uncommitted allocation and all traces of it
vanish at crash, its number can be used again; there is no live file to overwrite.
This is distinct from reusing a name whose old inode may still be reachable.

New WALs and SSTable temporary files are created exclusively. Before SSTable
rename, the final name must still be absent. Allocation/collision errors abort
and fail the engine rather than overwrite a file. Temporary SSTables are named
sst-NNNNNN.sst.tmp and installed as sst-NNNNNN.sst. Only MANIFEST is intentionally
replaced, through the specified atomic temporary-file installation. Recovery
unlinks temporary aliases and syncs the directory before MANIFEST.tmp can be
created again; it never truncates an alias of a live installed manifest.

The manifest's SSTable list is oldest to newest. Names are unique and file IDs
increase; positive sequence ranges must be disjoint and strictly increasing:
previous.max_seq < next.min_seq. Every range is at or below flushed_seq; every
referenced ID is below next_file_number. Gaps are valid because overwrites
retain only the latest memtable entry. The watermark can exceed the newest
retained table's max_seq (the final format permits future full compaction to
remove tombstones), and a nonzero watermark with zero tables is valid. M2 does
not itself produce that zero-table state or drop deletion markers.

### Complete validation and recovery barriers

All SSTable integers are little-endian. Header magic/version are LSMSST01/1;
the fixed 36-byte footer ends with LSMEND01. The body CRC covers header and all
entries. Validation checks the complete header/footer, checksum, exact entry
boundaries, strictly ascending unique keys, unique positive entry sequences,
legal operations and sizes, zero delete value length, count, and actual min/max
sequences. Referenced file size and bounds must agree with the manifest. Empty
SSTables are invalid: empty flush emits no table. Loaded tables are cached in
memory; their installed files are never modified.

Full manifest validation checks framing, UTF-8 JSON, checksum, exact fields,
integer types/bounds, canonical unique names, positive legal sizes, sequence
ranges/list ordering, watermark bounds, and allocation bounds. Generation zero
must be unflushed. A noninitial generation with zero watermark cannot be produced
by any supported writer and retains the prior UnsupportedFormatError rejection.

Recovery validates the manifest, every referenced SSTable, and all WALs before
any orphan cleanup or WAL retirement. This strengthens §9's original cleanup
ordering and preserves M1's no-deletion-on-WAL-corruption behavior. A missing
referenced SSTable is CorruptionError. No WAL is fatal at any generation. Bad
metadata or a required-file failure leaves temporary/orphan/covered files intact.
Unknown ordinary files are rejected; *.tmp and unreferenced sst-* are cleaned
only after required-file validation succeeds.

Validated WALs are replayed in numeric file order with the M1 conservative tail
rules. Only records strictly above flushed_seq enter the memtable. Highest valid
WAL sequence and the manifest watermark determine next_seq; referenced SSTable
maxima are already bounded by that watermark. The numerically last WAL is active
and is never retired, even if empty or completely covered. Non-active WALs are
retired only when every record is covered (including genuinely empty old WALs).

Before destructive cleanup, recovery syncs replayed/repaired WAL files and the
current directory image. In particular, a reopen without a crash may see a valid
new manifest rename left unsynced by a failed flush: this barrier makes that
visible manifest durable before its watermark can authorize WAL retirement.
Cleanup unlinks then syncs the directory, preventing stale temporary aliases or
retirements from being reordered with later installations. Crashes during these
steps either retain redundant files or recover the same complete state.

### Flush behavior and reads

`Engine.open(fs, memtable_bytes=4 MiB, wal_bytes=16 MiB)` accepts positive integer
byte thresholds. Live-entry accounting is 17 + len(key) + len(value) per latest
memtable entry, including tombstones; overwrites subtract the previous entry.
The WAL counter includes its header and all encoded records. After append sync
and memtable application, either threshold triggers a flush before acknowledgment.
A single large legal entry may exceed the live threshold; the WAL trigger may
overshoot by the record that crossed it. Recovered counters reflect the actual
valid active WAL and current live memtable. Open itself does not force a flush.
`flush()` on an empty memtable issues no filesystem calls and rotates nothing.

A nonempty flush freezes the serial memtable, then performs exactly this order:
1. Exclusively create the next WAL, write/sync its header, and sync the directory.
2. Write/sync the new SSTable temporary file, rename it, and sync the directory.
3. Write/sync MANIFEST.tmp with incremented generation, appended SSTable metadata,
   advanced allocation counter and frozen maximum as flushed_seq; rename and
   sync the directory. Only then clear the frozen memtable and publish its cache.
4. Retire covered non-active WALs and sync the directory. The new active WAL stays.

Any exception during a partially executed flush marks the engine failed; later
put/get/delete/flush/snapshot calls reject until close and a fresh open. No write
or sync is retried. Earlier acknowledged writes remain covered by old WALs until
the new manifest is durable; errors after that point leave a valid installed
state and possibly redundant old WALs. Reads check memtable first, then tables
newest to oldest, stopping on a tombstone as well as a value.

### Whole-state oracle and coverage

`Engine.snapshot()` is a diagnostic copy of the entire visible dictionary,
including keys unknown to the workload. M1/M2 recovery helpers now use that full
image instead of projecting onto a known-key list; History already compares
whole dictionaries. A regression demonstrates that unexpected extra keys are
rejected, rather than silently removed before comparison. Snapshot is for test
introspection; it is not an on-disk range index or a scan implementation.

The tiny one-entry explicit flush enumerates 999 before/after-call/completion
and persistence choices, including duplicate images. Repeated recovery tests
start from flush interruptions before manifest installation and before WAL
retirement: respectively 33/35 first recovery crashes and 1041/1145 second
recovery crashes are enumerated. Each second crash is followed by a third open
and full-state comparison. These are exhaustive only for those finite supplied
traces, not every possible flush/workload or every combination of earlier crashes.

Three 35-operation reference workloads use seeds 3, 17, and 42 with low live/WAL
thresholds; crash outcomes sample seed and seed+1000 at every reached boundary.
Additional tests cover both triggers, repeated overwrite live accounting, empty
flush, oversized legal input (retained M1 tests), multiple SSTables and deletion
markers, watermark filtering including zero-table fixtures, required-file damage,
manifest/actual-table metadata disagreements, orphan/temporary cleanup, temporary
manifest aliases, allocator collision refusal, and the six/seven-digit boundary.
Before/after EIO injection covers every flush create/write/sync/rename/unlink;
every byte prefix of small WAL-header/SSTable/manifest flush writes is tested for
partial-write errors and reopening without a crash. No retry is permitted.

Four source mutations were applied independently and restored in finally blocks.
The same `test_flush_at_every_call_and_modeled_crash_outcome` rejected all four:
- Omitting SSTable installation directory sync: a new manifest referenced a
  missing sst-000003.sst after a permitted reordered namespace crash.
- Omitting manifest installation directory sync: old WAL retirement persisted
  without the new manifest, losing the acknowledged dictionary entry.
- Delaying SSTable data sync until after manifest installation: the referenced
  table recovered without a complete header/footer.
- Deleting the required old WAL before SSTable/manifest installation: recovery
  returned an empty dictionary instead of the acknowledged value.
The unmodified test passed after restoration. These are deliberate mutation
checks, not invented production defects. BUGS.md records the actual projection
and numeric-order defects that failed their regressions before fixes.

Remaining limits: SimFS and injected EIO semantics, CRC32 collision limits,
single serial owner, whole-WAL/table reads and cached tables, no real OS sync
or SIGKILL validation, and no compaction or performance guarantees. Exhaustive
prefix/namespace enumeration grows quickly; larger scenarios sample explicit
recorded seeds. Durable ordering and the acknowledged-write contract are unchanged.

## 21. M3 full-merge compaction

`Engine.compact()` merges every currently installed SSTable using their validated
in-memory entries. For each key it explicitly retains the version with the
highest sequence, then drops a winning tombstone. This is safe because all
installed older tables participate and the replacement manifest references none
of the inputs. Unreferenced orphan files are not live versions and are never
consulted. The existing recovery watermark filter remains mandatory: old WAL
puts must not reenter the memtable once their table tombstones have been dropped.

Compaction is not an implicit flush. It does not rotate/write/retire WALs, allocate
sequence numbers, change live-byte/WAL-byte accounting, or modify any memtable
entry. Memtable values and tombstones stay newer than the compacted table. The
manifest's flushed_seq remains exactly unchanged even if the highest surviving
SSTable sequence decreases after dropping a tombstone. Existing recovery derives
next_seq from that watermark and valid WALs, so deleted acknowledged sequence
numbers are not reused.

### Installation and empty-output policy

For nonempty output, the shared M2 allocator reserves one fresh SSTable ID.
Compaction exclusively creates its temporary file, writes and syncs it, verifies
that the final name is absent, renames it, and syncs the directory. The replacement
manifest increments generation, lists only that output, preserves flushed_seq,
and records the advanced next_file_number. The existing manifest installer writes
and syncs MANIFEST.tmp, renames it, and syncs the directory. Only after that barrier
does compaction replace the in-memory table cache and unlink every input file.
A final directory sync makes those deletions durable. Filesystem operations all
use FileSystem; a partially completed operation never overwrites an input table.

If all merged winners are tombstones, compaction writes no SSTable and allocates
no ID. It installs a manifest with zero SSTables, incremented generation, and
unchanged flushed_seq and next_file_number, then deletes inputs in the same safe
order. The in-memory allocator retains any higher floor observed during recovery;
empty compaction does not reset it to the stored counter. This preserves the
M2 protection against reusing an observed orphan's number in the same session.

With no installed SSTables, compact() is a no-op issuing no filesystem calls,
even when the memtable contains newer data. With one or more tables, every call
performs a full merge and installs a new manifest; nonempty output uses a fresh
ID even if one input already has no tombstones. There is no partial compaction
or automatic compaction trigger in M3.

An exception during compaction marks the Engine failed, including after a
successful manifest install but a failed input unlink or final directory sync.
No barrier or failed mutation is retried. Subsequent data/maintenance/snapshot
calls reject until close and a new open/recovery. InjectedCrash bypasses normal
Exception cleanup as before. A crash before manifest installation leaves an
orphan output and valid inputs. A crash afterward leaves valid replacement state
and possibly orphan inputs. M2 validation-before-cleanup recovery handles both,
including zero-SSTable manifests and dirty namespace reopens after an I/O error.

### Validation and deliberate mutations

Tests check overwrites/deletes across multiple input tables, newer memtable
values and tombstones, dropping the highest persisted tombstone while preserving
the watermark, all-deleted output, repeated empty/nonempty compaction, a later
flush after compaction, output-name collision refusal, and allocator floors from
observed orphan files. Redundant old WAL fixtures are reintroduced through the
filesystem boundary after compaction to verify that discarded tombstones cannot
be resurrected. Both nonempty and zero-table states are checked with the
whole-dictionary History oracle, including unexpected extra keys.

Automatic compaction crash tests enumerate 961 nonempty-output and 415 empty-output
boundary/persistence choices, including duplicate images. Both include newer
acknowledged memtable data. Repeated recovery tests start before manifest rename
and before input deletion, for both output types. They enumerate:

| Interrupted state | First recovery crashes | Second recovery crashes |
| --- | ---: | ---: |
| Before manifest, nonempty | 33 | 1041 |
| Before manifest, empty | 27 | 729 |
| Before input deletion, nonempty | 41 | 1461 |
| Before input deletion, empty | 39 | 1301 |

Every second recovery crash is followed by another open and full-state comparison.
These are exhaustive for each supplied finite case/trace only, not a Cartesian
product of every possible compaction crash and two recovery crashes. Separate
seeded workloads use seeds 5 and 19, 25 operations each, low flush thresholds,
and repeated full compaction, sampling crash seeds seed and seed+1000 per reached
boundary. Large workload coverage is sampled, not exhaustive.

Before/after EIO injection covers every reached compaction filesystem call for
both output types. Every byte prefix of small output and manifest writes is
injected as a partial-write error, including zero/full prefixes, then reopened
without a crash. Error tests also enumerate crash images and reopen with the dirty
state still visible. Compaction-derived recovery states inject errors in reads,
writes/repairs when present, file/directory syncs, and orphan unlinks, then reopen
with and without crashes. Later calls are rejected after failure and no failing
barrier is retried.

Six independent source mutations were tested, with the original restored in a
finally block. Premature input deletion failed on a missing still-referenced
sst-000003.sst. Omitting output file sync failed on an incomplete replacement
SSTable; omitting output directory sync failed on a missing sst-000006.sst.
Omitting manifest file sync failed on a short manifest header. Omitting manifest
directory sync allowed an old manifest to survive after an input disappeared.
Each of those five mutations failed the nonempty compaction crash test.
Disabling recovery's watermark filter failed both redundant-WAL tests: the
whole-state oracle observed b'gone': b'old' after that key's tombstone was discarded,
including after an all-deleted compaction. The four targeted test cases passed
again after restoration. These are deliberate validation defects, not entries
invented for BUGS.md; no additional non-injected implementation defect was found
in this milestone's tests/review, so the factual defect log is unchanged.

### Limits and change record

The on-disk format is unchanged. M3 adds full-merge compaction only. It uses
in-memory dictionaries and whole encoded output buffers, with no streaming or
performance guarantee. Input tables are the cached images validated at open (or
installation), matching existing reads; M3 adds no continuous corruption monitor.
The SimFS prefix/namespace assumptions, CRC32 collision limitation, serial single
owner, lack of a real OS adapter/SIGKILL testing, and bounded small-case exhaustive
coverage remain. No indexes, Bloom filters, partial compaction, or group commit
were introduced.

Change record: implemented §10 with explicit one-table/no-table policies,
preserved memtable/WAL state and the empty-output allocation floor, and recorded
compaction/recovery crash, EIO, partial-write, and mutation-check coverage. The
watermark and acknowledged-write durability guarantees were not weakened.

## 22. M4 implementation decisions and validation

This section supersedes the earlier milestone deferrals of a real adapter, CLI,
process-kill testing, and CI. The on-disk WAL, SSTable, and manifest formats and
installation ordering are unchanged. No new storage-engine features are added.

### Real filesystem binding and descriptor ownership

RealFS binds one absolute database-directory path and its existing parent.
ensure_directory creates only the database directory; sync_parent fsyncs the
parent descriptor separately from sync_dir. Parent provisioning remains the
caller's responsibility. Names are single components. Operations use directory
file descriptors, O_NOFOLLOW for storage entries, and O_CLOEXEC. Data operations
open and close their own descriptors. Exclusive create uses O_CREAT|O_EXCL;
writes loop over short results and retry InterruptedError, rejecting zero
progress. Truncation uses ftruncate. Same-directory os.replace supplies atomic
manifest replacement; the engine's fresh-ID checks protect non-manifest files
from filename reuse. Replacement alone is not a durability barrier.

Linux file barriers use os.fsync; macOS file barriers require
fcntl.F_FULLFSYNC. Directory and parent-directory barriers use os.fsync on the
respective descriptors. RealFS exposes sync_primitive,
directory_sync_primitive, and parent_sync_primitive; CLI info and child-test
records report them. Missing required Python/OS primitives, or required barrier
calls rejected as unsupported, raise DurabilityUnavailableError. Other I/O errors
propagate. A failed barrier is never explicitly retried or replaced by a weaker
primitive. Python's normal syscall interruption behavior still applies. The
macOS policy follows §1; there is no fsync-only macOS fallback.

Locks are advisory, exclusive, nonblocking flock locks held by dedicated open
file descriptions until unlock/close. LOCK stays in the namespace; cooperating
owners must never unlink or replace it while an owner is alive. Losing openers
close only their own descriptors. Reopening an already-open adapter does not
release a live owner's lock; same-adapter duplicate locking is rejected. Ordinary
file reads/closes do not release the dedicated lock. Lock tokens are adapter-local.
Descriptors are non-inheritable across exec. Inherited adapter use after fork is
rejected; children create fresh adapters. Closing inherited copies closes their
references without issuing LOCK_UN against a parent's shared description.

close is idempotent, attempts every owned descriptor, and performs no implicit
sync. Explicit close/context management reports cleanup failures. A best-effort
finalizer reclaims abandoned adapters, including failed inline Engine.open calls;
correct lifetime management does not rely on timely garbage collection.

### Real-directory and child-process validation

Native integration tests cover initialization, exclusive creation, complete
writes, close/reopen, competing openers, descriptor ownership, flush, nonempty and
empty compaction, sequence recovery, corruption rejection before orphan cleanup,
and interrupted tail repair. Injected primitive-unavailability checks verify
explicit errors and no weaker fallback. A tracked-descriptor regression exposed
and fixed an abandoned-adapter leak, recorded in BUGS.md.

SIGKILL tests use explicit started/run/completed/ACK handshakes and at most one
outstanding command. The parent tracks acknowledged history and an optional
in-flight mutation; after child death it checks the whole recovered dictionary
with History, including unexpected keys. Maintenance commands preserve history.
Default seeds 11 and 29 cover selected WAL-write, flush, nonempty-compaction,
empty-compaction, final-ACK, and free-running-write kills. Separate tests kill
recovery after truncation and kill the next recovery before file sync, then
complete recovery. Timeout cleanup verifies killing/reaping and lock release.

The harness records seeds, operations, acknowledgment status, method/call/phase
checkpoints, sync primitives, kill location, runtime, and stderr as JSON. Checkpoint
kills are deterministic selected boundaries, not exhaustive enumeration. A
free-running case uses a seeded delay with its exact syscall location explicitly
unknown; scheduling is not reproducible from the seed. Timeouts bound protocol
waits and process cleanup. TESTING.md gives commands and report locations.

Process death leaves OS caches intact. These tests establish process-recovery
behavior for the exercised schedules, not power-loss durability. SimFS remains
the separate, bounded modeled power-loss and missing-barrier test layer. Real
barriers rely on truthful OS/filesystem/device behavior; no hardware power cycle
or network-filesystem guarantee is established here.

### CLI, CI, and scope

The lsmkv console entry point and python -m lsmkv accept --db and put/get/delete,
flush/compact/info commands. Keys/values use strict even-length hex, with empty
strings representing empty bytes. Results are JSON; exit codes are 0 success,
1 absent get, 2 argument/storage/lock/corruption errors. Every command opens and
recovers the database, initializing an absent directory. README documents install,
examples, limits, and primitive reporting. Storage runtime dependencies remain
standard-library only.

CI is configured to run the full suite and installed CLI help on Linux/macOS with Python 3.11/3.14,
records versions, and preserves JUnit and child traces, including failed jobs.
Recorded native validation used Darwin 24.6.0 arm64 / Python 3.14.7.
Separately, [GitHub Actions run 41521655829](https://github.com/aslesha-kp/lsm-key-value-engine/actions/runs/41521655829)
succeeded on Ubuntu/macOS with Python 3.11/3.14: each of the four jobs ran 282
tests with zero skips, failures, or errors, confirmed by uploaded JUnit reports.
Native suites skip only unsupported operating systems; unsupported required
durability primitives fail explicitly.
No partial compaction, indexes, Bloom filters, group commit, or replication.

Change record: implemented M4 around the existing filesystem interface; specified
real descriptor/lock lifetime, strict unavailable-primitive errors, hex CLI
semantics, and the distinction between process death and modeled power loss.
Durability and recovery guarantees are unchanged.

## 23. Reproducible measurements

The optional standard-library lsmkv.benchmark module uses RealFS without changing
engine behavior or per-operation durability barriers. It records fresh-directory
repetitions of one fixed workload: durable initial puts, a multi-table overwritten
and deleted dataset with newer WAL entries, before/after-compaction existing and
missing queries, clean reopen/recovery, and full compaction. Whole-state and query
checks occur outside timed regions. Defaults, timer boundaries, raw samples,
nearest-rank percentiles, environment/sync metadata, operation/query fingerprints,
and exact reproduction commands are documented in benchmarks/README.md.

SSTables are cached in memory; lookup measurements are not cold disk reads.
Reopen retains OS caches and includes Engine.open validation and recovery
barriers. These small measurements do not establish large-data performance or
power-loss durability. No storage formats, indexes, filters, commit policy, or
replication were added. TESTING.md records local validation and benchmark
environments separately from the four successful jobs in
[GitHub Actions run 41521655829](https://github.com/aslesha-kp/lsm-key-value-engine/actions/runs/41521655829).
