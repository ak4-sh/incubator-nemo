# Original Sponge HoloStream port

This branch starts at the last shared Sponge commit:

- Nemo base: `4c976182e4a4ae18d740143212f6881347d162b6e`
- Beam base: `f5c1f2a516`
- Beam HoloStream adapter head: `62eefa340b`
- Beam worktree: `../beam-original-sponge-holostream`

The goal is to run the original Sponge scaling and graph-rewriting behavior on
CloudLab while feeding standard Beam Nexmark Query 6 with the unchanged
HoloStream Auction and Bid Kafka records.

## Port boundary

Allowed additions are limited to:

1. Build and Java compatibility needed by the current environment.
2. CloudLab worker provisioning, addressing, and strict placement.
3. A single logical, multi-topic Beam Kafka source for HoloStream MUS tuples.
4. Passive metrics and reproducible experiment harnesses.

The original `IRDAG.insert(SrcStreamVertex, ...)` assumed that an unbounded
source always produced exactly the policy-requested number of readables. The
HoloStream adapter deliberately exposes one readable per Kafka partition (8)
while Query 6 remains at parallelism 4. The source relay therefore inherits
the source parallelism, and the pre-existing RoundRobin edge performs the
8-to-4 handoff. This is a source-boundary compatibility correction; it does
not alter the scaler or the R1/R2/R3 policy logic.

The scaler, migration policy, backpressure policy, R1/R2/R3 rewrites, and
watermark/task execution paths are protected. Run
`scripts/cloudlab/verify_original_sponge_behavior.sh` after every porting
change. If a protected file must change to make the experiment run, record the
failure first and place the behavioral change in a separate, explicitly named
commit.

## Input telemetry contract

At the base commit, `InputAndCpuBasedScaler.addCurrentInput` treats every
`INPUT n` message as a new interval count and adds it to the aggregate input.
The HoloStream harness must therefore write per-poll Kafka offset deltas to
`source.log`. Cumulative offsets belong in a separate diagnostics file. Run
completion is validated against the telemetry CSV's cumulative `totalEvents`
column, never the final interval delta in `source.log`.

## Query boundary

Beam must expose one `PCollection<Event>` source to Nemo:

```text
Kafka topics [Auction, Bid]
  -> topic-aware MUS decoder
  -> one PCollection<Event>
  -> unmodified Beam Nexmark Query 6
```

Separate reads followed by a source-level `Flatten` are intentionally
forbidden because they introduce an artificial multi-root topology that was
not present in the original Sponge evaluation path.

## Implemented experiment entry points

- Scaling-disabled validation: `scripts/cloudlab/run_original_sponge_q6_smoke.sh`
- Native scale-out trial: `scripts/cloudlab/run_original_sponge_q6_scaleout.sh`

The scaling-disabled validation does not provision VM workers or send an
`add-lambda-executor` command. The scale-out entry point explicitly enables
offloading and owns warm-pool provisioning. This prevents a baseline smoke
test from inadvertently scheduling transient-path work on offload workers.

The scale-out wrapper uses four offload hosts with 27 one-core workers per host
(108 workers total). Its `start-scaler` command only arms the original scaler;
the scaler still makes the scale-out decision from the unmodified queue and CPU
conditions. The command is sent before live production begins.

`WorkerControlProxy` emits `SPONGE_VM_WORKERS` transition records with the
current active worker count. This is passive telemetry only. Initial warm-pool
registration is followed by deactivation; the count after that point measures
workers activated by Sponge's scale-out path.

## Build validation

Beam must be compiled with Java 8 and Nemo with Java 11. The adapter test is
`HoloStreamEventDecoderTest`; it covers Go-generated Auction/Bid byte fixtures,
topic discrimination, one decode per record, timestamp reuse, and invalid
records. The CloudLab artifact verifier additionally rejects duplicate Beam
classes across the Nemo client and Nexmark application jars.
