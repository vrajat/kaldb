# KalDB NRT Hybrid Log Benchmark

This benchmark exercises KalDB's near-real-time live replication path with an
`http_logs`-style workload. It continuously ingests synthetic log documents,
runs recurring search and aggregation queries, captures NRT publication
artifacts, stops the active indexer, starts a replacement indexer, and checks
whether recent data remains queryable through cache-backed live segments.

The initial execution target is Docker Compose only.

## Scope

Dataset tiers:

- `100MB`
- `1GB`
- `5GB`
- `10GB`

Document shape:

- `@timestamp`
- `host`
- `service`
- `status`
- `method`
- `path`
- `bytes`
- `message`
- `run_id`
- `seq_id`

Recurring query suite:

- recent time-range filter on one service and host
- error-rate filter with `status >= 500`
- top terms aggregation on `service`
- date histogram over the last `N` minutes
- message text filter on a synthetic phrase
- exact document lookup by `run_id` and `seq_id`
- latest visible `seq_id` lookup for freshness tracking

## Prerequisites

Build the KalDB image first:

```bash
docker build -t slackhq/astra .
```

Recommended local tools:

- `docker compose`
- `python3`
- `curl`

Optional tools used when available:

- `aws` for S3-compatible object listings

## Run

The benchmark entrypoint always writes under `tools/nrt-bench/artifacts/<run_id>/`.

```bash
tools/nrt-bench/run.sh --tier 100MB
```

Choose the storage backend explicitly:

```bash
tools/nrt-bench/run.sh --tier 100MB --storage-backend minio
tools/nrt-bench/run.sh --tier 1GB --storage-backend s3 \
  --s3-bucket my-kaldb-bench-bucket \
  --s3-region us-east-1 \
  --s3-endpoint https://s3.us-east-1.amazonaws.com
```

Useful overrides:

```bash
tools/nrt-bench/run.sh \
  --tier 1GB \
  --storage-backend minio \
  --seed 42 \
  --query-cadence 5 \
  --restart-fraction 0.55 \
  --artifact-root tools/nrt-bench/artifacts
```

To reuse an already-running benchmark cluster instead of recreating it:

```bash
tools/nrt-bench/run.sh --tier 100MB --no-compose-up
```

## Storage Backend

`--storage-backend minio`

- default mode
- starts local `minio` and `minio_setup` services from `docker-compose.nrt-bench.yml`
- points every KalDB service at `S3_ENDPOINT=http://minio:9000`
- uses the local host endpoint `http://127.0.0.1:19090` for harness-side object listing
- bootstraps the configured bucket automatically through `mc mb --ignore-existing`

`--storage-backend s3`

- does not start the local `minio` services
- points every KalDB service at the value passed through `--s3-endpoint`
- passes `--s3-bucket`, `--s3-region`, `--s3-access-key`, and `--s3-secret-key` through to KalDB
- uses the same endpoint and bucket for harness-side object listing
- if you omit `--s3-endpoint`, object listing falls back to the AWS CLI when available

## How The Backend Flows Into KalDB

The flow is:

1. `run.sh` calls `runner/harness.py`.
2. `harness.py` parses `--storage-backend`.
3. The harness sets Docker Compose env vars:
   - `KALDB_S3_ENDPOINT`
   - `KALDB_S3_BUCKET`
   - `KALDB_S3_REGION`
   - `KALDB_S3_ACCESS_KEY`
   - `KALDB_S3_SECRET_KEY`
   - `S3_PATH_PREFIX`
4. `docker-compose.nrt-bench.yml` injects those into every KalDB container as:
   - `S3_ENDPOINT`
   - `S3_BUCKET`
   - `S3_REGION`
   - `S3_ACCESS_KEY`
   - `S3_SECRET_KEY`
5. KalDB reads those env vars through [config/config.yaml](/home/rajat/code/kaldb/config/config.yaml:34), which maps them into `s3Config`.
6. The same harness value also controls whether the local MinIO services are started at all.

So the backend switch affects two things at once:

- which object store endpoint KalDB itself writes live NRT manifests to
- whether the benchmark bootstraps a local object store or expects an external one

## Outputs

Each run produces:

- `run-config.json`
- `events.ndjson`
- `summary.json`
- `report.md`
- `logs/<service>.log`
- `metrics/<service>/<timestamp>.prom`
- `docker-stats/<timestamp>.ndjson`
- `s3/<timestamp>.json`
- copied benchmark config inputs under `inputs/`

## Status Model

The harness reports one of:

- `pass`: observed NRT publish progress, cache searchability evidence, bounded
  query continuity, and post-restart forward progress
- `fail`: the scenario ran but one or more success criteria failed
- `blocked`: the branch under test did not demonstrate the required cache-side
  live replication behavior, so the benchmark is not considered valid

`blocked` is used intentionally when the run cannot show cache-served live
continuity, rather than silently degrading to an indexer-only benchmark.
