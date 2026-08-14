# NRT Development Tracker

This file tracks benchmark-driven NRT work that should stay split into small,
reviewable changes. The benchmark can expose product bugs, but those fixes
should not be blocked behind one large NRT benchmark review.

## Current Baseline

- Harness-only reliability work is committed as `2c809458 Improve NRT benchmark harness reliability`.
- AWS S3/profile benchmark support is committed as
  `c2ae15f7 Support AWS S3 profile credentials in NRT bench`.
- Remaining uncommitted changes are Astra product/runtime fixes and their tests.
- Latest observed benchmark artifact: `tools/nrt-bench/artifacts/nrt-bench-100mb-20260813T173311Z`.
- That run completed ingest, published NRT manifests to AWS S3, and reached cache searchability, but
  reported `fail` because NRT publishing did not quiesce within `300s`; latest visible seq lag was
  `22183` docs against a `400` doc budget.

## Review Buckets

### Harness Reliability

Status: committed, with additional freshness-reporting changes ready to commit.

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
- Treat freshness lag during active NRT publication differently from lag after
  publication has gone quiet.

Validation:
- `python3 -m py_compile tools/nrt-bench/runner/harness.py`
- `git diff --check -- tools/nrt-bench/...`

Latest validation:
- `tools/nrt-bench/artifacts/nrt-bench-100mb-20260813T173311Z` classified the failure as
  `timeout_still_active`, which means the harness exited instead of hanging and correctly identified
  active NRT publish/catch-up as the limiting path.

### AWS S3 Backend Support

Status: committed as `c2ae15f7 Support AWS S3 profile credentials in NRT bench`.

Files:
- `tools/nrt-bench/runner/harness.py`
- `tools/nrt-bench/docker-compose.nrt-bench.yml`
- `tools/nrt-bench/README.md`
- `astra/pom.xml`
- `astra/src/main/java/com/slack/astra/blobfs/S3AsyncUtil.java`
- `astra/src/test/java/com/slack/astra/blobfs/S3AsyncUtilTest.java`

Bug:
- Native AWS S3 runs initially looked like credential or upload failures, but KalDB containers were
  still receiving `S3_ENDPOINT=http://minio:9000`.
- Docker Compose expansion used `${KALDB_S3_ENDPOINT:-http://minio:9000}`. The harness passed an
  intentionally empty endpoint for native AWS S3, and `:-` treated that empty value as unset.
- The indexers then tried to upload to MinIO, which was not started in `--storage-backend s3` mode.

Fix shape:
- Leave `S3_ENDPOINT` empty for native AWS S3 and only pass `http://minio:9000` when the harness is
  running with `--storage-backend minio`.
- Do the same empty-safe handling for `S3_ACCESS_KEY` and `S3_SECRET_KEY` so blank values select the
  AWS SDK default credential chain instead of fake static credentials.
- Add `--profile`, AWS region envs, `AWS_SDK_LOAD_CONFIG`, session-token env propagation, and a
  host AWS config mount at `/root/.aws`.
- Keep the AWS config mount writable for SSO profiles. The Java SDK correctly selects
  `ProfileCredentialsProvider`, and that provider may refresh/write entries under
  `~/.aws/sso/cache` during startup.
- Add AWS SDK `sso` and `ssooidc` modules so Java SDK v2 can resolve IAM Identity Center / SSO
  profile credentials.
- Log which S3 credential provider path was selected without logging credential values.

Java-side notes:
- No functional change was required to the core S3 upload path. `S3AsyncUtil` already selected
  `DefaultCredentialsProvider` when static access/secret keys were absent.
- Java changes were still required for observability and classpath completeness:
  - `S3AsyncUtil` logs `StaticCredentialsProvider` vs `DefaultCredentialsProvider` and the resolved
    credential type/provider name.
  - `astra/pom.xml` includes the AWS SSO credential modules needed by profile-backed runs.
- The current config default still has `s3EndPoint: ${S3_ENDPOINT:-http://localhost:9090}` in
  `config/config.yaml`. Benchmark containers avoid that default by explicitly setting
  `S3_ENDPOINT=`. A broader product cleanup could make native AWS S3 less surprising by changing the
  default endpoint to empty, but that would affect non-benchmark local defaults and should be a
  separate config review.

Validation:
- `python3 -m py_compile tools/nrt-bench/runner/harness.py`
- `docker compose -p kaldb_nrt_bench -f tools/nrt-bench/docker-compose.nrt-bench.yml config`
- Native S3 compose expansion shows `S3_ENDPOINT: ""`.
- MinIO compose expansion shows `S3_ENDPOINT: http://minio:9000`.
- `mvn -pl astra -Dtest=S3AsyncUtilTest test`
- `mvn -pl astra -DskipTests compile`

Follow-ups:
- Consider a focused Java/config test for native AWS S3 config semantics: blank endpoint plus blank
  static keys should produce a default AWS S3 client using `DefaultCredentialsProvider`.
- Consider changing product config defaults so native AWS S3 does not require explicitly passing an
  empty `S3_ENDPOINT`; do this only with a broader local-dev compatibility review.
- Keep provider logging, but avoid expanding it into credential material or account-specific output.

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

### NRT Publish Throughput

Status: needs product investigation.

Observed behavior:
- `tools/nrt-bench/artifacts/nrt-bench-100mb-20260813T173311Z` ingested `265000` docs with no
  ingest failures or query errors.
- During freshness validation, `astra_index_p1` continued publishing manifests for partition `1`,
  but the visible sequence stayed pinned at `242816`.
- The run timed out with `timeout_still_active`: manifest publication was still moving at the
  `300s` visibility deadline, and lag remained `22183` docs.

Likely areas to inspect:
- NRT publish loop throughput and per-generation file upload cost.
- Whether publishing too many tiny generations is creating S3/listing overhead.
- Cache catch-up and visibility refresh cadence after manifest publication.

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
