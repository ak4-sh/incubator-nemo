# HoloStream producer inputs for Sponge

This directory owns the Sponge side of the HoloStream Kafka integration.
HoloStream remains an external, unchanged producer.

## Upstream producer

- Repository: `holostream`
- Reference commit: `2217278aad5a3775503c53316a93761efc4bfc28`
- Executable: `<WorkDir>/bin/nexmarkKafkaProducer`
- Kafka topics supplied by the unchanged producer:
  - `nexmark-auction`
  - `nexmark-bid`

The launcher records and compares deployed producer checksums. A reference
binary checksum is intentionally not recorded here until the clean upstream
commit is rebuilt; supply that run's checksum on the command line.

## Formal workload

The producer configuration is
`config/sponge_q6_formal.json`. It is compatible with the unchanged HoloStream
configuration schema: it does not use topic overrides or
`ExitOnCompletion`.

Eight replicas collectively produce:

| Phase | Duration | Auction rate | Bid rate | Combined rate |
| --- | ---: | ---: | ---: | ---: |
| 1 | 100 s | 1,224/s | 18,776/s | 20,000/s |
| 2 | 150 s | 6,120/s | 93,880/s | 100,000/s |
| 3 | 150 s | 12,248/s | 187,752/s | 200,000/s |

Each replica produces 359,700 Auctions and 5,515,300 Bids. Global expected
topic counts are:

- Auctions: 2,877,600
- Bids: 44,122,400
- Total: 47,000,000

Because the unchanged HoloStream executable remains alive after bounded
generation, the Sponge launcher must detect the expected Kafka offsets and
then terminate the producer replicas.

## Sponge launcher

`run_producers.py` launches and supervises the unchanged producer. It verifies
that the executable is present and byte-identical on every producer host. Pass
the checksum recorded for the HoloStream comparison run with
`--expected-producer-sha256` to enforce that both systems used the exact same
binary. It also requires the fixed topics to exist with eight partitions and
to be empty. The launcher copies the configuration to each producer host,
starts replicas 0 through 7, and verifies exact per-partition completion:

- every `nexmark-auction` partition reaches 359,700 records;
- every `nexmark-bid` partition reaches 5,515,300 records.

Example:

```bash
python3 scripts/cloudlab/holostream/run_producers.py \
  --config scripts/cloudlab/holostream/config/sponge_q6_formal.json \
  --run-id q6-holostream-formal-1 \
  --output-dir /tmp/q6-holostream-formal-1 \
  --kafka-node node1 \
  --kafka-home /users/akash01/kafka \
  --bootstrap-servers node1:9092,node2:9092,node3:9092 \
  --expected-producer-sha256 <sha256-from-holostream-run>
```

After Kafka reaches the exact offsets, the launcher reads each run-specific
remote PID file, verifies its command line, and terminates only that producer.
It writes:

- `holostream_producer_completion.json`;
- `holostream_producer_replicas.csv`;
- one log per producer replica.

Validate the copied workload without contacting Kafka or producer hosts:

```bash
python3 scripts/cloudlab/holostream/run_producers.py \
  --config scripts/cloudlab/holostream/config/sponge_q6_formal.json \
  --run-id validation \
  --output-dir /tmp/unused \
  --validate-only
```

The main Sponge harness invokes this launcher directly when
`PRODUCER_IMPL=holostream`; no free-form producer command is required. It first
stores `holostream_producer_plan.json` in the run work directory and derives
the Beam bounded-source counts and `producer_phases.csv` schedule from that
validated plan. The harness requires `HOLOSTREAM_EXPECTED_PRODUCER_SHA256` so
the Sponge run cannot silently use a different producer binary.

Before launching Sponge, the harness requires `HOLOSTREAM_RESET_TOPICS=true`
and runs `reset_topics.py`. That utility is hardcoded to
`nexmark-auction` and `nexmark-bid`: it captures their existing metadata,
deletes and recreates them, verifies the configured partition count and
`LogAppendTime`, and requires beginning and end offset zero on every partition.
The evidence is stored in `holostream_topic_reset.json`. An atomic control-node
lock remains held until the harness exits, preventing another managed run from
resetting these fixed topics concurrently.

During the run, the metrics collector writes aggregate Kafka fields to
`combined_metrics.csv` and per-topic rows to `kafka_topic_metrics.csv`.
Aggregate offsets and lag are emitted only when both Auction and Bid samples
are valid. Queue-residence time remains an aggregate across source tasks
because those task-level metrics do not currently include Kafka topic identity.
Kafka CLI polling runs locally on the control node. After production, the
harness waits for stable expected offsets, source count, and zero committed
lag, preserves raw node metrics, writes `final_validation.json`, and stops the
collector before the launching session exits.

## Wire fixtures

`fixtures/auction.mus.hex` and `fixtures/bid.mus.hex` are MUS-encoded records
used by the Java decoder tests. They are copied into Sponge so decoder
validation does not depend on a dirty HoloStream checkout.
