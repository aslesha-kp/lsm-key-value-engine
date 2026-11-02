# Defects and regressions

Resolved defects; deliberate mutation checks are documented in the design.

## Simulated inode identity

Combining filename maps hid an overwritten inode from crash choices. Tracking
reachable inode identities separately restores those outcomes. Rename also
removed a source name when both names referenced one inode; it now treats that
case as a no-op.
Regressions: `test_overwritten_inode_remains_selectable_for_crash` and
`test_rename_between_names_of_same_inode_is_noop` in [test_m0.py](tests/test_m0.py).

## Filesystem error types

Missing simulated files leaked KeyError. Centralized lookup now raises
FileNotFoundError for reads, deletion, and rename.
Regression: `test_missing_files_report_filesystem_errors` in
[test_m0.py](tests/test_m0.py).

## Quadratic pending-write memory

Per-byte full-file snapshots used 134,920,575 traced bytes for a 16 KiB pending
write. Compact write/truncate records preserve crash prefixes with linear storage.
Regression: `test_pending_write_memory_is_linear_small_regression`, plus tiny
prefix comparisons and a 2 MiB case in [test_harness.py](tests/test_harness.py).

## Crash-runner boundaries

Filesystem-only checkpoints missed acknowledgment after the final call. Adding
completion checkpoints detects acknowledged unsynced writes. Separately, cleanup
could sync after an injected crash; latching the crash now rejects subsequent I/O.
Regressions: `test_runner_checks_acknowledgment_after_final_fs_call` and
`test_crash_does_not_allow_finally_to_complete_a_sync` in
[test_harness.py](tests/test_harness.py).

## Interrupted initialization

The original missing-manifest rule rejected a reachable state containing
MANIFEST.tmp and a header-only first WAL. Initialization accepts a temporary
manifest beside a short or validated header-only WAL, while rejecting record
bytes and invalid complete headers without deleting files.
Regression: `test_missing_manifest_strictness_and_approved_tmp_exception` in
[test_recovery_crashes.py](tests/test_recovery_crashes.py).

## Whole-state recovery checks

A known-key projection hid unexpected recovered keys. Engine.snapshot() and
whole-dictionary comparisons now expose them.
Regression: `test_known_key_projection_cannot_hide_an_extra_recovered_key` in
[test_m2.py](tests/test_m2.py).

## Numeric WAL ordering

Lexical sorting replayed WAL 1000000 before 999999. Recovery now sorts parsed
numeric IDs for replay, active-WAL selection, and allocation.
Regression: `test_wal_replay_and_active_selection_use_numeric_file_order` in
[test_m2.py](tests/test_m2.py).

## Abandoned adapter descriptors

A failed inline Engine.open(RealFS(path)) leaked database and parent directory
descriptors when the adapter was discarded. Best-effort no-sync finalization now
reclaims them; explicit close/context management remains the deterministic path.
Regression: `test_failed_inline_engine_open_does_not_leak_directory_descriptors`
in [test_realfs.py](tests/test_realfs.py).
