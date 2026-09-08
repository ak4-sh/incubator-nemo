# Sine and worker-reactivation experiment archive

Curated on 2026-09-08. These are historical runs, not new experiments or validation
of the current source checkout. No Java compilation or cluster run was performed
while archiving them.

## Outcome index

| Run | Recorded evidence | Classification and limit |
| --- | --- | --- |
| [60K–450K sine, 20260813T1733Z](sponge-q6-sine-60k450k-20260813T1733Z/final_validation.json) | 60,150,000 inputs drained; historical terminal validator passed; no scale-out wait found in the archived driver trace. | Input-drain/no-scale observation, not proof of R3 offloading or Q6 output correctness. |
| [60K–450K, 45-second peak](sponge-q6-sine-60k450k-peak45-20260813T2057Z/RUN_SUMMARY.md) | 73,650,000 inputs drained; summary reports zero scale-out decisions. | Input-drain/no-scale observation under the historical validator. |
| [100K–550K Figure 9b](sponge-q6-figure9b-scaled-100k550k-20260813T214837Z/RUN_SUMMARY.md) | 37,510,000 inputs drained; summary reports zero scale-out decisions. | Input-drain/no-scale observation, not a successful scaling experiment. |
| [118K–650K Figure 9b](sponge-q6-figure9b-scaled-118k650k-20260813T220549Z/RUN_SUMMARY.md) | 44,329,660 inputs drained; summary reports zero scale-out decisions. | Input-drain/no-scale observation, not a successful scaling experiment. |
| [164K–900K Figure 9b](sponge-q6-figure9b-scaled-164k900k-20260813T222409Z/RUN_SUMMARY.md) | 61,379,940 inputs drained, but the task dispatcher failed while activating a DEACTIVATING worker during the second decision. | **Failed scaling run.** The original `final_validation.json` says `validated: true`; that obsolete offset-based verdict does not override the documented crash. |
| [c6525 cap124 lifecycle check](sponge-q6-c6525-deferred-reactivation-cap124-20260828T174230Z/worker_lifecycle_validation.json) | Lifecycle validator passed: workers 36/37/88/120 reached two reactivations; worker 88's deferred request was released; all 148 END sends were acknowledged. | **Scoped lifecycle pass only.** No overall `final_validation.json` is present in this archive; do not infer an end-to-end Q6 pass. |
| [c6525 100K–375K repeated scale](sponge-q6-c6525-repeated-scale-100k375k-20260828T191034Z/final_validation.json) | 150,000,000 inputs drained; lifecycle and overall validation failed. Required reuse depth 3 was not reached; invocation logs disagreed. | **Failed/incomplete Q6 scaling validation.** Later investigation also found merger migration and unsupported worker-to-worker pipes. |

The old result JSON, CSVs, plots, and run summaries are preserved without changing
their recorded values. `rawMetricsPreserved: true` in an old report refers to what
was preserved locally at collection time, not to the completeness of this Git archive.
An empty `yarn_application_status.json` in the 164K–900K archive is deliberately
retained and flagged in its manifest; it is not a valid JSON status report.

## What is in Git

- Original metrics, producer plans and phase records, validation reports, placement
  records, runtime commands, plots, and available summaries.
- Driver stderr traces for all seven runs, including both captured driver copies
  in the 164K–900K archive.
- Selected c6525 VM-worker logs: workers 36/37/88/120 from the lifecycle check and
  workers 76/87/106/108/120/123 from the repeated-scale run. These preserve the
  deferred transition, reuse evidence, invocation discrepancies, and pipe failures.
- One `ARCHIVE_MANIFEST.json` per run, listing each original file's relative path,
  byte count, SHA-256, and storage decision (`git` or `local-only`).

## What remains local only

- Other bulk evaluator/worker logs, stdout captures, and compressed log bundles.
- Approximately 327.7 MB (312.5 MiB) across 376 files; none was deleted or moved.
- Approximately 33.4 MB of original evidence was selected for Git, plus the
  generated manifests and this index.
- **Local-only files are not backed up by Git, and this archive does not assert a
  separate remote backup.** Their exact paths and checksums are in each manifest.
- A fresh checkout contains selected logs, not all inputs needed to rerun the
  full original lifecycle validator. The recorded validation JSON is preserved;
  complete revalidation needs the corresponding local-only logs too.

Manifests cover the original files present before curation; generated manifests
and this index are not self-hashed. Archived artifact hashes identify historical
JARs where available. Committing the results alongside current source does not
prove those historical binaries match the current source revision.

## Interpretation and current status

- The lifecycle fix is distinct from the unresolved Q6 controller/protocol issue.
- `start-scaler` currently invokes whole-task migration; it does not exercise the
  separately available R3 redirection command simply because R1/R2/R3 rewrites ran.
- Zero Kafka lag, worker activations, or a lifecycle-check pass alone do not prove
  successful Q6 partial-state offloading, downstream correctness, or paper-policy fidelity.
- See [reactivation status](../../DEFERRED_WORKER_REACTIVATION.md) and the
  [migration re-investigation](../../SPONGE_Q6_MIGRATION_REINVESTIGATION_20260907.md).
  The latter's line references refer to the investigated source and original logs;
  some full worker logs intentionally remain local only as described above.
