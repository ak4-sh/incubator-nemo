# Original Sponge / HoloStream experiment commit manifest

This manifest records the paired repositories used for the preserved standard
Nexmark Query 6 experiment. HoloStream itself remains unchanged; its producer
is launched and supervised by the experiment harness in this repository.

## Beam

- Repository: `../beam-original-sponge-holostream`
- Branch: `experiment/original-beam-holostream`
- Original Sponge Beam base: `f5c1f2a516`
- Java 8 build compatibility: `0d6f7c8ed1`
- Partition-aligned Kafka splits: `41836874f7`
- HoloStream MUS adapter head: `62eefa340b`

## Sponge / Nemo

- Repository: this repository
- Branch: `experiment/original-sponge-holostream`
- Original Sponge base: `4c976182e`
- Partition-aligned source relay: `708be306e`
- CloudLab runtime port: `5374916ad`
- Passive source telemetry: `912ec0459`
- Reproducible CloudLab Q6 harness: `468fe0952`

The protected original Sponge scaler, R1/R2/R3 rewrite policy, migration
policy, backpressure policy, watermark path, and task execution path continue
to match `4c976182e` as checked by
`scripts/cloudlab/verify_original_sponge_behavior.sh`.

## Preserved experiment evidence

- HoloStream memory and Pebble runs: `6ea506378`
- Successful Sponge 225k to 450k scale-out run: `5125a6bc9`
- Validation run and three-system comparison: `dbf7bf6fb`

The archived validation files contain the SHA-256 hashes of the compiled Nemo
and Beam/Nexmark artifacts used by the successful run.
