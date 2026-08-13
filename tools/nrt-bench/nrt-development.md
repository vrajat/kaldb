# NRT Development Tracker

This file tracks benchmark-driven NRT work that should stay split into small,
reviewable changes. The benchmark can expose product bugs, but those fixes
should not be blocked behind one large NRT benchmark review.

## Current Baseline

- Harness-only reliability work is committed as `2c809458 Improve NRT benchmark harness reliability`.
- Remaining uncommitted changes are Astra product/runtime fixes and their tests.
- Latest observed benchmark artifact: `tools/nrt-bench/artifacts/nrt-bench-100mb-20260813T071539Z`.
- That run completed ingest but reported `blocked` because cache searchability and replacement
  publish criteria were not satisfied.

## Review Buckets

### Harness Reliability

Status: committed.

Files:
- `tools/nrt-bench/runner/harness.py`
- `tools/nrt-bench/docker-compose.nrt-bench.yml`
- `tools/nrt-bench/workloads/dataset-tiers.json`
- `tools/nrt-bench/README.md`

Purpose:
- Start a complete two-partition compose topology.
- Create the Kafka topic with the requested partition count.
- Avoid blocking ingest on synchronous query/log collection.
- Add command timeouts and serialized compose command execution.
- Pace ingest from workload throughput defaults and retry rate-limit responses.
- Capture enough logs, metrics, and S3 state to explain blocked runs.

Validation:
- `python3 -m py_compile tools/nrt-bench/runner/harness.py`
- `git diff --check -- tools/nrt-bench/...`

### Preprocessor Timeout

Status: candidate product fix.

Files:
- `astra/src/main/java/com/slack/astra/bulkIngestApi/BulkIngestKafkaProducer.java`
- `astra/src/test/java/com/slack/astra/bulkIngestApi/BulkIngestKafkaProducerTest.java`

Bug:
- In the non-transactional producer path, a producer-side exception created an error response but
  did not attach it to the waiting `BulkIngestRequest`.
- The HTTP request path then waited until request timeout, which made a small request look like a
  preprocessor stall.

Fix shape:
- Build a response for both success and error paths.
- Call `request.setResponse(...)` after the `try/catch` so every request wakes its waiter.

Validation:
- `mvn -pl astra -Dtest=BulkIngestKafkaProducerTest#testNonTransactionalProducerCompletesWaitingRequestOnError test`

### Distributed Query Live Routing

Status: candidate product fix.

Files:
- `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
- `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`

Bug:
- If a snapshot had multiple live/indexer search nodes and no cache-hosted node, routing tried to
  pick randomly from an empty cache-node list.

Fix shape:
- Preserve cache preference when cache nodes exist.
- If no cache nodes exist, pick from all queryable live nodes.

Validation:
- `mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testMultipleLiveSearchNodesWithoutCache test`

### Cache LIVE Snapshot Lookup

Status: candidate product fix, needs careful review.

Files:
- `astra/src/main/java/com/slack/astra/chunk/ReadOnlyChunkImpl.java`
- `astra/src/test/java/com/slack/astra/chunk/ReadOnlyChunkImplTest.java`

Bug:
- Cache assignment for an NRT live replica can fail with `Error finding node at path LIVE_...`.
- `SnapshotMetadataStore.findSync(snapshotId)` searches only partitions already known in the local
  partitioned metadata store.
- Live snapshots are stored in the fixed `LIVE` partition, so a cache-side metadata store can miss
  the partition if it was constructed before discovery/watcher state caught up.

Fix shape:
- Keep the existing `findSync` path first.
- If it fails for a `LIVE_` snapshot id, directly read `snapshotMetadataStore.getSync("LIVE", id)`.

Validation:
- `mvn -pl astra -Dtest=ReadOnlyChunkImplTest#shouldLoadLiveReplicaIntoMissingSlotDirectory test`

### Indexer Shutdown Ordering

Status: candidate product fix, highest review risk.

Files:
- `astra/src/main/java/com/slack/astra/server/Astra.java`
- `astra/src/main/java/com/slack/astra/server/AstraIndexer.java`
- `astra/src/main/java/com/slack/astra/writer/kafka/AstraKafkaConsumer.java`

Bug:
- A benchmark shutdown showed NRT publish and Lucene cleanup racing:
  `NoSuchFileException` during NRT upload, followed by `IndexWriter should never be null when
  adding a message`.
- `AstraIndexer` and `IndexingChunkManager` are separate services. During process shutdown, the
  chunk manager can close chunks while the indexer is still polling or processing Kafka records.

Fix shape:
- Stop `AstraIndexer` services first in the shutdown hook.
- Wake the Kafka consumer during indexer shutdown so `poll()` exits promptly.
- Then stop the rest of the service manager.

Validation so far:
- `mvn -pl astra -Dtest=AstraIndexerTest#testIndexFreshConsumerKafkaSearchViaGrpcSearchApi test`

Open concern:
- This changes service shutdown ordering and should be reviewed separately from NRT/harness work.

## Benchmark Interpretation Notes

- `Replacement indexer never published a new NRT generation` is not necessarily a product bug.
- In the observed run, the replacement indexer started with `READ_FROM_LOCATION_ON_START=LATEST`
  after ingest had already completed, so it had no new Kafka records to index or publish.
- The harness should either keep ingesting after replacement starts, start replacement earlier, or
  change the pass/fail criterion for this scenario.

## Suggested Commit Order

1. Harness reliability: already committed.
2. Preprocessor timeout fix.
3. Distributed query live routing fix.
4. Cache `LIVE` snapshot lookup fix.
5. Indexer shutdown ordering fix.
