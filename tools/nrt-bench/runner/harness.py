#!/usr/bin/env python3
"""Orchestrates the Docker Compose NRT benchmark and captures run artifacts."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import logging
import os
import random
import socket
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = CURRENT_DIR.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from collectors.log_summary import parse_service_log


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], pct: float) -> float | None:
    """Returns a simple linear percentile for a non-empty list."""
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def json_dump(path: Path, payload: Any) -> None:
    """Writes JSON with stable formatting."""
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier", required=True, choices=["100MB", "1GB", "5GB", "10GB"])
    parser.add_argument("--storage-backend", choices=["minio", "s3"], default="minio")
    parser.add_argument("--dataset", default="nrt-bench")
    parser.add_argument("--owner", default="nrt-bench@local")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--query-cadence", type=float, default=5.0)
    parser.add_argument("--restart-fraction", type=float, default=0.55)
    parser.add_argument("--bulk-url", default="http://127.0.0.1:18086/_bulk")
    parser.add_argument("--query-url", default="http://127.0.0.1:18081")
    parser.add_argument("--manager-url", default="http://127.0.0.1:18083")
    parser.add_argument("--artifact-root", default=str(PACKAGE_ROOT / "artifacts"))
    parser.add_argument("--compose-file", default=str(PACKAGE_ROOT / "docker-compose.nrt-bench.yml"))
    parser.add_argument("--compose-project", default="kaldb_nrt_bench")
    parser.add_argument("--config-file", default=str(PACKAGE_ROOT / "configs" / "nrt-bench.env"))
    parser.add_argument("--tier-file", default=str(PACKAGE_ROOT / "workloads" / "dataset-tiers.json"))
    parser.add_argument("--query-suite-file", default=str(PACKAGE_ROOT / "workloads" / "query-suite.json"))
    parser.add_argument("--s3-endpoint", default="")
    parser.add_argument("--s3-bucket", default="nrt-bench-bucket")
    parser.add_argument("--s3-region", default="us-east-1")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--astra-partition-count", type=int, default=2)
    parser.add_argument("--service-check-timeout", type=int, default=240)
    parser.add_argument("--kafka-ready-timeout", type=int, default=120)
    parser.add_argument("--restart-grace-seconds", type=int, default=10)
    parser.add_argument("--resume-timeout", type=int, default=240)
    parser.add_argument("--ingest-max-retries", type=int, default=3)
    parser.add_argument("--ingest-retry-backoff-seconds", type=float, default=5.0)
    parser.add_argument("--compose-up", dest="compose_up", action="store_true")
    parser.add_argument("--no-compose-up", dest="compose_up", action="store_false")
    parser.add_argument("--teardown", action="store_true")
    parser.set_defaults(compose_up=True)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    """Loads a JSON object from disk."""
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass
class CommandResult:
    """Captured result for a local subprocess invocation."""

    args: list[str]
    returncode: int
    stdout: str
    stderr: str
    started_at: str
    finished_at: str
    cwd: str | None
    label: str | None = None


class BenchmarkCommandError(RuntimeError):
    """Raised when a subprocess used by the benchmark fails."""

    def __init__(self, result: CommandResult) -> None:
        self.result = result
        label = f" [{result.label}]" if result.label else ""
        super().__init__(
            f"Command failed{label} with exit code {result.returncode}: {' '.join(result.args)}"
        )


def run_command(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture_output: bool = True,
) -> CommandResult:
    """Runs a subprocess and returns decoded output."""
    started_at = iso_now()
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=capture_output,
    )
    result = CommandResult(
        args=args,
        returncode=completed.returncode,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
        started_at=started_at,
        finished_at=iso_now(),
        cwd=str(cwd) if cwd else None,
    )
    if check and result.returncode != 0:
        raise BenchmarkCommandError(result)
    return result


def http_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: str | bytes | None = None,
    timeout: int = 20,
) -> tuple[int, str]:
    """Executes an HTTP request and returns status code and decoded body."""
    payload = body.encode("utf-8") if isinstance(body, str) else body
    request = urllib.request.Request(url, data=payload, method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.getcode(), response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def wait_for_http(url: str, timeout_seconds: int) -> None:
    """Polls an HTTP health endpoint until success or timeout."""
    deadline = time.monotonic() + timeout_seconds
    last_error = "unknown"
    while time.monotonic() < deadline:
        try:
            status, _ = http_request("GET", url, timeout=5)
            if 200 <= status < 300:
                return
            last_error = f"http {status}"
        except Exception as exc:  # pragma: no cover - best effort diagnostics
            last_error = str(exc)
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {url}: {last_error}")


def wait_until(deadline_seconds: int, interval_seconds: float, fn: Any) -> None:
    """Retries a callable until it succeeds or the deadline is reached."""
    deadline = time.monotonic() + deadline_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            fn()
            return
        except Exception as exc:  # pragma: no cover - best effort diagnostics
            last_error = exc
            time.sleep(interval_seconds)
    if last_error is None:
        raise TimeoutError("Timed out waiting for condition")
    raise TimeoutError(f"Timed out waiting for condition: {last_error}") from last_error


def render_template(value: Any, context: dict[str, Any]) -> Any:
    """Recursively formats strings in a JSON-like object with the given context."""
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, list):
        return [render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_template(item, context) for key, item in value.items()}
    return value


def parse_ndjson_bulk_response(response_text: str) -> dict[str, Any]:
    """Normalizes the KalDB bulk response into counts the harness can track."""
    payload = json.loads(response_text)
    total_docs = int(payload.get("totalDocs", 0))
    failed_docs = int(payload.get("failedDocs", 0))
    return {"total_docs": total_docs, "failed_docs": failed_docs, "raw": payload}


def extract_hit_count(payload: dict[str, Any]) -> int:
    """Extracts OpenSearch-style hit counts from a query response."""
    hits = payload.get("hits", {})
    total = hits.get("total")
    if isinstance(total, dict):
        return int(total.get("value", 0))
    if isinstance(total, int):
        return total
    hit_list = hits.get("hits")
    if isinstance(hit_list, list):
        return len(hit_list)
    return 0


def extract_latest_seq(payload: dict[str, Any]) -> int | None:
    """Extracts the latest visible seq_id from a search response."""
    hits = payload.get("hits", {}).get("hits", [])
    if not hits:
        return None
    source = hits[0].get("_source", {})
    seq_value = source.get("seq_id")
    if seq_value is None:
        return None
    return int(seq_value)


def find_latest_publish(events: list[dict[str, Any]], service: str | None = None) -> dict[str, Any] | None:
    """Finds the most recent NRT publish event."""
    matches = [
        event
        for event in events
        if event.get("type") == "nrt_publish" and (service is None or event.get("service") == service)
    ]
    if not matches:
        return None
    return max(matches, key=lambda event: int(event.get("generation", 0)))


def list_s3_objects(
    endpoint: str,
    bucket: str,
    prefix: str,
    env: dict[str, str],
    output_path: Path,
) -> list[str]:
    """Lists S3-compatible objects via aws cli if available, else unsigned XML requests."""
    aws_path = shutil.which("aws")
    if aws_path:
        command = [aws_path]
        if endpoint:
            command.extend(["--endpoint-url", endpoint])
        command.extend(["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix])
        try:
            result = run_command(command, env=env)
            output_path.write_text(result.stdout, encoding="utf-8")
            payload = json.loads(result.stdout)
            return [item["Key"] for item in payload.get("Contents", [])]
        except BenchmarkCommandError:
            if not endpoint:
                raise

    query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix})
    url = f"{endpoint.rstrip('/')}/{bucket}?{query}"
    status, body = http_request("GET", url, timeout=20)
    if not 200 <= status < 300:
        raise RuntimeError(f"Failed to list S3 objects from {url}: {status}")
    output_path.write_text(body, encoding="utf-8")
    root = ET.fromstring(body)
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    keys = [node.text or "" for node in root.findall(".//s3:Key", namespace)]
    if not keys:
        keys = [node.text or "" for node in root.findall(".//Key")]
    return keys


@dataclass
class RunState:
    """Mutable run state shared by the orchestrator threads."""

    dataset: str
    run_id: str
    artifact_dir: Path
    latest_ingested_seq: int = -1
    latest_visible_seq: int = -1
    ingest_bytes_sent: int = 0
    ingest_docs_sent: int = 0
    ingest_failed_docs: int = 0
    ingest_retry_count: int = 0
    query_errors: int = 0
    query_successes: int = 0
    first_publish_generation: int | None = None
    first_publish_path: str | None = None
    restart_requested: bool = False
    indexer_stopped_at: str | None = None
    replacement_started_at: str | None = None
    resume_publish_at: str | None = None
    s3_object_history: list[dict[str, Any]] = field(default_factory=list)
    query_latencies_ms: list[float] = field(default_factory=list)
    publish_generations: list[int] = field(default_factory=list)
    cache_searchable_hosts: set[str] = field(default_factory=set)


class EventWriter:
    """Serializes NDJSON event writes from concurrent collectors."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

    def write(self, event_type: str, **payload: Any) -> None:
        record = {"ts": iso_now(), "type": event_type, **payload}
        encoded = json.dumps(record, sort_keys=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(encoded + "\n")


class MetricsCollector(threading.Thread):
    """Scrapes service /metrics endpoints on a fixed interval."""

    def __init__(
        self,
        metrics_dir: Path,
        services: dict[str, str],
        stop_event: threading.Event,
        event_writer: EventWriter,
        interval_seconds: int = 5,
    ) -> None:
        super().__init__(daemon=True)
        self.metrics_dir = metrics_dir
        self.services = services
        self.stop_event = stop_event
        self.event_writer = event_writer
        self.interval_seconds = interval_seconds

    def run(self) -> None:
        while not self.stop_event.is_set():
            stamp = iso_now().replace(":", "").replace("-", "")
            for service, url in self.services.items():
                service_dir = self.metrics_dir / service
                service_dir.mkdir(parents=True, exist_ok=True)
                target = service_dir / f"{stamp}.prom"
                try:
                    status, body = http_request("GET", url, timeout=10)
                    target.write_text(body, encoding="utf-8")
                    self.event_writer.write(
                        "metrics_scrape",
                        service=service,
                        status=status,
                        path=str(target.relative_to(self.metrics_dir.parent)),
                    )
                except Exception as exc:
                    target.write_text(str(exc) + "\n", encoding="utf-8")
                    self.event_writer.write(
                        "metrics_scrape_error",
                        service=service,
                        error=str(exc),
                        path=str(target.relative_to(self.metrics_dir.parent)),
                    )
            self.stop_event.wait(self.interval_seconds)


class DockerStatsCollector(threading.Thread):
    """Captures periodic docker stats snapshots when docker is available."""

    def __init__(
        self,
        stats_dir: Path,
        stop_event: threading.Event,
        event_writer: EventWriter,
        compose_file: Path,
        compose_project: str,
        interval_seconds: int = 10,
    ) -> None:
        super().__init__(daemon=True)
        self.stats_dir = stats_dir
        self.stop_event = stop_event
        self.event_writer = event_writer
        self.compose_file = compose_file
        self.compose_project = compose_project
        self.interval_seconds = interval_seconds

    def run(self) -> None:
        while not self.stop_event.is_set():
            stamp = iso_now().replace(":", "").replace("-", "")
            target = self.stats_dir / f"{stamp}.ndjson"
            try:
                containers = run_command(
                    [
                        "docker",
                        "compose",
                        "-p",
                        self.compose_project,
                        "-f",
                        str(self.compose_file),
                        "ps",
                        "-q",
                    ]
                ).stdout.split()
                if containers:
                    result = run_command(
                        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers]
                    )
                    target.write_text(result.stdout, encoding="utf-8")
                    self.event_writer.write(
                        "docker_stats",
                        path=str(target.relative_to(self.stats_dir.parent)),
                        containers=len(containers),
                    )
            except Exception as exc:
                target.write_text(str(exc) + "\n", encoding="utf-8")
                self.event_writer.write(
                    "docker_stats_error",
                    path=str(target.relative_to(self.stats_dir.parent)),
                    error=str(exc),
                )
            self.stop_event.wait(self.interval_seconds)


class S3Collector(threading.Thread):
    """Polls the configured S3-compatible prefix and tracks object growth."""

    def __init__(
        self,
        s3_dir: Path,
        stop_event: threading.Event,
        event_writer: EventWriter,
        endpoint: str,
        bucket: str,
        prefix: str,
        env: dict[str, str],
        state: RunState,
        interval_seconds: int = 5,
    ) -> None:
        super().__init__(daemon=True)
        self.s3_dir = s3_dir
        self.stop_event = stop_event
        self.event_writer = event_writer
        self.endpoint = endpoint
        self.bucket = bucket
        self.prefix = prefix
        self.env = env
        self.state = state
        self.interval_seconds = interval_seconds

    def run(self) -> None:
        while not self.stop_event.is_set():
            stamp = iso_now().replace(":", "").replace("-", "")
            target = self.s3_dir / f"{stamp}.json"
            try:
                keys = list_s3_objects(self.endpoint, self.bucket, self.prefix, self.env, target)
                manifest_keys = [key for key in keys if "/manifests/" in key]
                snapshot = {"ts": iso_now(), "keys": keys, "manifest_keys": manifest_keys}
                self.state.s3_object_history.append(snapshot)
                self.event_writer.write(
                    "s3_poll",
                    path=str(target.relative_to(self.s3_dir.parent)),
                    object_count=len(keys),
                    manifest_count=len(manifest_keys),
                )
            except Exception as exc:
                target.write_text(str(exc) + "\n", encoding="utf-8")
                self.event_writer.write(
                    "s3_poll_error",
                    path=str(target.relative_to(self.s3_dir.parent)),
                    error=str(exc),
                )
            self.stop_event.wait(self.interval_seconds)


class BenchmarkHarness:
    """Owns one end-to-end benchmark run."""

    SERVICES = {
        "preprocessor": "http://127.0.0.1:18086/metrics",
        "index": "http://127.0.0.1:18080/metrics",
        "manager": "http://127.0.0.1:18083/metrics",
        "query": "http://127.0.0.1:18081/metrics",
        "cache_a": "http://127.0.0.1:18082/metrics",
        "cache_b": "http://127.0.0.1:28082/metrics",
        "recovery": "http://127.0.0.1:18085/metrics",
    }

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.compose_file = Path(args.compose_file).resolve()
        self.compose_project = args.compose_project
        self.config_file = Path(args.config_file).resolve()
        self.tier_file = Path(args.tier_file).resolve()
        self.query_suite_file = Path(args.query_suite_file).resolve()
        self.artifact_root = Path(args.artifact_root).resolve()
        self.tier_config = load_json(self.tier_file)["tiers"][args.tier]
        self.workload_config = load_json(self.tier_file)["field_distributions"]
        self.query_suite = load_json(self.query_suite_file)["queries"]
        self.run_started_at = iso_now()
        timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"nrt-bench-{args.tier.lower()}-{timestamp}"
        self.s3_prefix = args.s3_prefix or f"runs/{self.run_id}"
        self.kaldb_s3_endpoint = (
            "http://minio:9000" if args.storage_backend == "minio" else args.s3_endpoint
        )
        self.host_s3_endpoint = (
            "http://127.0.0.1:19090" if args.storage_backend == "minio" else args.s3_endpoint
        )
        self.artifact_dir = self.artifact_root / self.run_id
        self.logs_dir = self.artifact_dir / "logs"
        self.inputs_dir = self.artifact_dir / "inputs"
        self.metrics_dir = self.artifact_dir / "metrics"
        self.stats_dir = self.artifact_dir / "docker-stats"
        self.s3_dir = self.artifact_dir / "s3"
        self.command_dir = self.artifact_dir / "commands"
        self.stop_event = threading.Event()
        self.event_writer = EventWriter(self.artifact_dir / "events.ndjson")
        self.state = RunState(dataset=args.dataset, run_id=self.run_id, artifact_dir=self.artifact_dir)
        self.current_phase = "init"
        self.command_counter = 0
        self.logger = logging.getLogger(f"kaldb.nrt_bench.{self.run_id}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.random = random.Random(args.seed)
        self.target_bytes = int(self.tier_config["target_bytes"])
        self.restart_trigger_bytes = int(self.target_bytes * args.restart_fraction)
        self.target_window_minutes = int(self.tier_config["query_window_minutes"])
        self.lag_budget_docs = int(self.tier_config["lag_budget_docs"])
        self.query_phrase = self.workload_config["message_phrase"]
        self.hot_service = self.workload_config["hot_service"]
        self.hot_host = self.workload_config["hot_host"]
        self.compose_env = os.environ.copy()
        self.compose_env["S3_PATH_PREFIX"] = self.s3_prefix
        self.compose_env["NRT_BENCH_S3_PREFIX"] = self.s3_prefix
        self.compose_env["KALDB_S3_ENDPOINT"] = self.kaldb_s3_endpoint
        self.compose_env["KALDB_S3_BUCKET"] = args.s3_bucket
        self.compose_env["KALDB_S3_REGION"] = args.s3_region
        self.compose_env["KALDB_S3_ACCESS_KEY"] = args.s3_access_key
        self.compose_env["KALDB_S3_SECRET_KEY"] = args.s3_secret_key
        self.compose_env["AWS_REGION"] = args.s3_region
        self.compose_env["AWS_ACCESS_KEY_ID"] = args.s3_access_key
        self.compose_env["AWS_SECRET_ACCESS_KEY"] = args.s3_secret_key

        if args.storage_backend == "s3" and not args.s3_endpoint and not shutil.which("aws"):
            raise ValueError(
                "--storage-backend s3 requires either --s3-endpoint for object listing "
                "or the aws CLI to be installed for AWS-native listing"
            )

    def configure_logger(self) -> None:
        if self.logger.handlers:
            return
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s phase=%(phase)s run_id=%(run_id)s %(message)s"
        )
        file_handler = logging.FileHandler(self.artifact_dir / "benchmark.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def log(self, level: int, message: str, *args: Any) -> None:
        self.logger.log(
            level,
            message,
            *args,
            extra={"phase": self.current_phase, "run_id": self.run_id},
        )

    def set_phase(self, phase: str) -> None:
        self.current_phase = phase
        self.log(logging.INFO, "Entering phase %s", phase)
        self.event_writer.write("phase", name=phase)

    def persist_command_result(self, result: CommandResult) -> None:
        self.command_counter += 1
        label = result.label or "command"
        safe_label = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in label)
        prefix = f"{self.command_counter:03d}-{safe_label}"
        json_dump(
            self.command_dir / f"{prefix}.json",
            {
                "args": result.args,
                "cwd": result.cwd,
                "returncode": result.returncode,
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "label": result.label,
            },
        )
        (self.command_dir / f"{prefix}.stdout.log").write_text(result.stdout, encoding="utf-8")
        (self.command_dir / f"{prefix}.stderr.log").write_text(result.stderr, encoding="utf-8")

    def exec_command(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        check: bool = True,
        label: str | None = None,
    ) -> CommandResult:
        self.log(logging.INFO, "Running command: %s", " ".join(args))
        result = run_command(args, cwd=cwd, env=env, check=False)
        result.label = label
        self.persist_command_result(result)
        self.log(logging.INFO, "Command finished rc=%s: %s", result.returncode, " ".join(args))
        if check and result.returncode != 0:
            self.log(logging.ERROR, "Command failed rc=%s: %s", result.returncode, " ".join(args))
            raise BenchmarkCommandError(result)
        return result

    def compose(self, *extra: str) -> CommandResult:
        label = f"docker-compose-{'-'.join(extra[:2])}" if extra else "docker-compose"
        return self.exec_command(
            ["docker", "compose", "-p", self.compose_project, "-f", str(self.compose_file), *extra],
            cwd=self.compose_file.parent,
            env=self.compose_env,
            label=label,
        )

    def prepare_artifacts(self) -> None:
        for directory in [
            self.artifact_dir,
            self.logs_dir,
            self.inputs_dir,
            self.metrics_dir,
            self.stats_dir,
            self.s3_dir,
            self.command_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        self.configure_logger()
        shutil.copy2(self.compose_file, self.inputs_dir / self.compose_file.name)
        shutil.copy2(self.config_file, self.inputs_dir / self.config_file.name)
        shutil.copy2(self.tier_file, self.inputs_dir / self.tier_file.name)
        shutil.copy2(self.query_suite_file, self.inputs_dir / self.query_suite_file.name)
        json_dump(
            self.artifact_dir / "run-config.json",
            {
                "run_id": self.run_id,
                "started_at": self.run_started_at,
                "tier": self.args.tier,
                "storage_backend": self.args.storage_backend,
                "dataset": self.args.dataset,
                "seed": self.args.seed,
                "query_cadence_seconds": self.args.query_cadence,
                "restart_fraction": self.args.restart_fraction,
                "restart_trigger_bytes": self.restart_trigger_bytes,
                "target_bytes": self.target_bytes,
                "lag_budget_docs": self.lag_budget_docs,
                "s3_bucket": self.args.s3_bucket,
                "s3_region": self.args.s3_region,
                "s3_endpoint": self.host_s3_endpoint,
                "s3_prefix": self.s3_prefix,
                "compose_file": str(self.compose_file),
                "config_file": str(self.config_file),
                "bulk_url": self.args.bulk_url,
                "query_url": self.args.query_url,
                "manager_url": self.args.manager_url,
                "astra_partition_count": self.args.astra_partition_count,
                "kafka_ready_timeout_seconds": self.args.kafka_ready_timeout,
            },
        )

    def create_doc(self, seq_id: int) -> dict[str, Any]:
        services = self.workload_config["services"]
        hosts = self.workload_config["hosts"]
        methods = self.workload_config["methods"]
        paths = self.workload_config["paths"]
        weighted_statuses: list[int] = []
        for key, weight in self.workload_config["status_weights"].items():
            weighted_statuses.extend([int(key)] * int(weight))

        service = self.hot_service if seq_id % 7 == 0 else self.random.choice(services)
        host = self.hot_host if seq_id % 5 == 0 else self.random.choice(hosts)
        status = weighted_statuses[self.random.randrange(len(weighted_statuses))]
        path = paths[(seq_id + self.random.randrange(len(paths))) % len(paths)]
        phrase = self.query_phrase if seq_id % 11 == 0 else "baseline log traffic"
        message = (
            f"{phrase} seq={seq_id} service={service} host={host} "
            f"status={status} method={methods[seq_id % len(methods)]}"
        )
        return {
            "@timestamp": utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "host": host,
            "service": service,
            "status": status,
            "method": methods[seq_id % len(methods)],
            "path": path,
            "bytes": 400 + (seq_id % 8192),
            "message": message,
            "run_id": self.run_id,
            "seq_id": seq_id,
        }

    def create_bulk_payload(self, start_seq_id: int) -> tuple[str, int, int]:
        batch_size = int(self.tier_config["batch_size"])
        lines: list[str] = []
        latest_seq = start_seq_id
        for seq_id in range(start_seq_id, start_seq_id + batch_size):
            lines.append(json.dumps({"index": {"_index": self.args.dataset, "_id": f"{self.run_id}-{seq_id}"}}))
            lines.append(json.dumps(self.create_doc(seq_id)))
            latest_seq = seq_id
        payload = "\n".join(lines) + "\n"
        return payload, batch_size, latest_seq

    def create_probe_bulk_payload(self) -> str:
        probe_seq_id = -1
        lines = [
            json.dumps({"index": {"_index": self.args.dataset, "_id": f"{self.run_id}-probe"}}),
            json.dumps(self.create_doc(probe_seq_id)),
        ]
        return "\n".join(lines) + "\n"

    def wait_for_ingest_ready(self) -> None:
        probe_payload = self.create_probe_bulk_payload()

        def probe() -> None:
            status, response_text = http_request(
                "POST",
                self.args.bulk_url,
                headers={"content-type": "application/x-ndjson"},
                body=probe_payload,
                timeout=12,
            )
            if 200 <= status < 300:
                self.event_writer.write("ingest_probe_ready", status=status)
                return
            self.event_writer.write(
                "ingest_probe_retry",
                status=status,
                body=response_text,
            )
            raise RuntimeError(f"Probe ingest not ready yet: HTTP {status}: {response_text}")

        wait_until(self.args.service_check_timeout, 2.0, probe)

    def ensure_partition_exists(self, partition_id: str, max_capacity: int) -> None:
        list_status, list_body = http_request(
            "POST",
            f"{self.args.manager_url}/slack.proto.astra.ManagerApiService/ListPartitionMetadata",
            headers={"content-type": "application/json; charset=utf-8; protocol=gRPC"},
            body=json.dumps({}),
        )
        if not 200 <= list_status < 300:
            raise RuntimeError(f"Failed to list partitions: {list_status} {list_body}")

        partition_payload = json.loads(list_body or "{}")
        partitions = partition_payload.get("partitionMetadata", [])
        if any(str(partition.get("partitionId")) == partition_id for partition in partitions):
            self.event_writer.write("manager_partition_exists", partition_id=partition_id)
            return

        create_status, create_body = http_request(
            "POST",
            f"{self.args.manager_url}/slack.proto.astra.ManagerApiService/CreatePartition",
            headers={"content-type": "application/json; charset=utf-8; protocol=gRPC"},
            body=json.dumps({"partitionId": partition_id, "maxCapacity": str(max_capacity)}),
        )
        if not 200 <= create_status < 300:
            raise RuntimeError(
                f"Failed to create partition {partition_id}: {create_status} {create_body}"
            )
        self.event_writer.write(
            "manager_partition_created",
            partition_id=partition_id,
            max_capacity=max_capacity,
            body=create_body,
        )

    def desired_partition_ids(self) -> list[str]:
        return [str(partition_id) for partition_id in range(self.args.astra_partition_count)]

    def configure_cluster(self) -> None:
        self.set_phase("preflight")
        if self.args.compose_up:
            self.compose("down", "-v", "--remove-orphans")
            base_services = ["zookeeper", "kafka", "openzipkin"]
            if self.args.storage_backend == "minio":
                base_services.extend(["minio", "minio_setup"])
            self.compose("up", "-d", *base_services)
            self.compose(
                "up",
                "-d",
                "astra_preprocessor",
                "astra_index",
                "astra_manager",
                "astra_query",
                "astra_cache_a",
                "astra_cache_b",
                "astra_recovery",
            )

        wait_for_http("http://127.0.0.1:18083/health", self.args.service_check_timeout)
        wait_for_http("http://127.0.0.1:18086/health", self.args.service_check_timeout)
        wait_for_http("http://127.0.0.1:18081/health", self.args.service_check_timeout)
        self.log(logging.INFO, "Service health checks passed")
        self.event_writer.write("health_ready", services=["manager", "preprocessor", "query"])

        self.log(logging.INFO, "Waiting for Kafka admin readiness before topic creation")
        wait_until(
            self.args.kafka_ready_timeout,
            2.0,
            lambda: self.compose(
                "exec",
                "-T",
                "kafka",
                "kafka-topics.sh",
                "--create",
                "--topic",
                "nrt-bench-topic",
                "--if-not-exists",
                "--bootstrap-server",
                "kafka:29092",
            ),
        )
        self.event_writer.write("kafka_topic_ready", topic="nrt-bench-topic")

        partition_capacity = max(int(self.tier_config["throughput_bytes"]) * 2, 1)
        partition_ids = self.desired_partition_ids()
        for partition_id in partition_ids:
            self.ensure_partition_exists(partition_id, partition_capacity)
        self.event_writer.write(
            "manager_partitions_ready",
            partition_ids=partition_ids,
            max_capacity=partition_capacity,
        )

        create_status, create_body = http_request(
            "POST",
            f"{self.args.manager_url}/slack.proto.astra.ManagerApiService/CreateDatasetMetadata",
            headers={"content-type": "application/json; charset=utf-8; protocol=gRPC"},
            body=json.dumps(
                {
                    "name": self.args.dataset,
                    "owner": self.args.owner,
                    "serviceNamePattern": self.args.dataset,
                }
            ),
        )
        self.event_writer.write("manager_create_dataset", status=create_status, body=create_body)

        assign_status, assign_body = http_request(
            "POST",
            f"{self.args.manager_url}/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment",
            headers={"content-type": "application/json; charset=utf-8; protocol=gRPC"},
            body=json.dumps(
                {
                    "name": self.args.dataset,
                    "throughputBytes": str(self.tier_config["throughput_bytes"]),
                    "partitionIds": partition_ids,
                }
            ),
        )
        if not 200 <= assign_status < 300:
            raise RuntimeError(f"Failed to assign dataset partitions: {assign_status} {assign_body}")
        self.event_writer.write("manager_assign_dataset", status=assign_status, body=assign_body)

        prefix_snapshot = self.s3_dir / "preflight-prefix.json"
        initial_keys = list_s3_objects(
            self.host_s3_endpoint, self.args.s3_bucket, self.s3_prefix, self.compose_env, prefix_snapshot
        )
        if initial_keys:
            raise RuntimeError(
                f"S3 prefix {self.s3_prefix!r} is not empty; refusing to run against stale artifacts"
            )
        self.event_writer.write("s3_prefix_empty", prefix=self.s3_prefix)
        self.log(logging.INFO, "Waiting for preprocessor ingest readiness after dataset assignment")
        self.wait_for_ingest_ready()

    def start_collectors(self) -> list[threading.Thread]:
        collectors: list[threading.Thread] = [
            MetricsCollector(self.metrics_dir, self.SERVICES, self.stop_event, self.event_writer),
            DockerStatsCollector(
                self.stats_dir,
                self.stop_event,
                self.event_writer,
                self.compose_file,
                self.compose_project,
            ),
            S3Collector(
                self.s3_dir,
                self.stop_event,
                self.event_writer,
                self.host_s3_endpoint,
                self.args.s3_bucket,
                self.s3_prefix,
                self.compose_env,
                self.state,
            ),
        ]
        for collector in collectors:
            collector.start()
        return collectors

    def wait_for_first_publish(self) -> None:
        deadline = time.monotonic() + self.args.resume_timeout
        while time.monotonic() < deadline:
            self.capture_logs()
            events = self.parse_all_logs()
            upload_errors = [event for event in events if event.get("type") == "s3_upload_error"]
            if upload_errors:
                first_error = upload_errors[0]
                raise RuntimeError(
                    "Indexer hit S3 upload errors before first NRT publish; "
                    f"service={first_error.get('service')} line={first_error.get('line')}"
                )
            first_publish = find_latest_publish(events)
            if first_publish:
                self.state.first_publish_generation = int(first_publish["generation"])
                self.state.first_publish_path = str(first_publish["path"])
                self.state.publish_generations.append(int(first_publish["generation"]))
                self.event_writer.write(
                    "first_publish_observed",
                    generation=first_publish["generation"],
                    path=first_publish["path"],
                )
                return
            time.sleep(5)
        raise RuntimeError("Timed out waiting for first NRT manifest publication")

    def build_query_context(self, seq_id: int | None = None) -> dict[str, Any]:
        now = utc_now()
        window_start = now - dt.timedelta(minutes=self.target_window_minutes)
        context = {
            "dataset": self.args.dataset,
            "host": self.hot_host,
            "service": self.hot_service,
            "run_id": self.run_id,
            "seq_id": str(seq_id if seq_id is not None else max(self.state.latest_ingested_seq, 0)),
            "time_gte": window_start.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "time_lte": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "message_phrase": self.query_phrase,
        }
        return context

    def run_query_suite(self, *, final_pass: bool = False) -> None:
        for query in self.query_suite:
            context = self.build_query_context(self.state.latest_ingested_seq)
            body = render_template(copy.deepcopy(query["body"]), context)
            if query["name"] == "exact_doc_lookup":
                body["query"]["bool"]["filter"][1]["term"]["seq_id"] = self.state.latest_ingested_seq
            endpoint = render_template(query["endpoint"], context)
            body_json = json.dumps(body)
            start = time.monotonic()
            status, response_text = http_request(
                "POST",
                f"{self.args.query_url}{endpoint}",
                headers={"content-type": "application/json"},
                body=body_json,
                timeout=20,
            )
            latency_ms = round((time.monotonic() - start) * 1000.0, 2)
            self.state.query_latencies_ms.append(latency_ms)
            if 200 <= status < 300:
                self.state.query_successes += 1
                payload = json.loads(response_text)
                hit_count = extract_hit_count(payload)
                latest_visible_seq = (
                    extract_latest_seq(payload) if query["name"] == "latest_visible_seq" else None
                )
                if latest_visible_seq is not None:
                    self.state.latest_visible_seq = latest_visible_seq
                exact_lookup_hit = hit_count > 0 if query["name"] == "exact_doc_lookup" else None
                self.event_writer.write(
                    "query_result",
                    name=query["name"],
                    final_pass=final_pass,
                    status=status,
                    latency_ms=latency_ms,
                    hit_count=hit_count,
                    latest_visible_seq=latest_visible_seq,
                    exact_lookup_hit=exact_lookup_hit,
                )
            else:
                self.state.query_errors += 1
                self.event_writer.write(
                    "query_error",
                    name=query["name"],
                    final_pass=final_pass,
                    status=status,
                    latency_ms=latency_ms,
                    body=response_text,
                )

    def maybe_restart_indexer(self) -> None:
        if self.state.restart_requested:
            return
        if self.state.ingest_bytes_sent < self.restart_trigger_bytes:
            return
        if self.state.first_publish_generation is None:
            return

        self.state.restart_requested = True
        self.state.indexer_stopped_at = iso_now()
        self.set_phase("failover")
        self.compose("stop", "astra_index")
        self.event_writer.write("indexer_stopped", at=self.state.indexer_stopped_at)
        time.sleep(self.args.restart_grace_seconds)
        self.state.replacement_started_at = iso_now()
        self.compose("up", "-d", "astra_index_replacement")
        self.event_writer.write("replacement_started", at=self.state.replacement_started_at)

    def wait_for_replacement_publish(self) -> None:
        if not self.state.replacement_started_at:
            return
        deadline = time.monotonic() + self.args.resume_timeout
        baseline = self.state.first_publish_generation or 0
        while time.monotonic() < deadline:
            self.capture_logs()
            events = self.parse_all_logs()
            replacement_publish = find_latest_publish(events, service="astra_index_replacement")
            if replacement_publish and int(replacement_publish["generation"]) > baseline:
                self.state.resume_publish_at = iso_now()
                self.state.publish_generations.append(int(replacement_publish["generation"]))
                self.event_writer.write(
                    "replacement_publish_observed",
                    generation=replacement_publish["generation"],
                    path=replacement_publish["path"],
                )
                return
            time.sleep(5)

    def duration_seconds(self, start_iso: str | None, end_iso: str | None) -> float | None:
        if not start_iso or not end_iso:
            return None
        start = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        return round((end - start).total_seconds(), 3)

    def tail_recent_events(self, limit: int = 20) -> list[dict[str, Any]]:
        if not self.event_writer._path.exists():
            return []
        lines = self.event_writer._path.read_text(encoding="utf-8").splitlines()
        tail = lines[-limit:]
        return [json.loads(line) for line in tail if line.strip()]

    def should_retry_ingest_error(self, status: int | None, error: Exception | None) -> bool:
        if status in {429, 500, 502, 503, 504}:
            return True
        return isinstance(error, (TimeoutError, urllib.error.URLError, socket.timeout))

    def post_bulk_batch(self, payload: str, batch_size: int) -> tuple[int, str]:
        attempts = max(1, self.args.ingest_max_retries)
        last_error: Exception | None = None
        last_status: int | None = None
        last_response_text = ""
        for attempt in range(1, attempts + 1):
            error: Exception | None = None
            try:
                status, response_text = http_request(
                    "POST",
                    self.args.bulk_url,
                    headers={"content-type": "application/x-ndjson"},
                    body=payload,
                    timeout=30,
                )
            except Exception as exc:
                status = None
                response_text = ""
                error = exc

            retryable = (
                error is not None
                or status is None
                or not 200 <= status < 300
                and self.should_retry_ingest_error(status, error)
            )
            if error is None and status is not None and 200 <= status < 300:
                return status, response_text

            last_error = error
            last_status = status
            last_response_text = response_text
            self.state.ingest_retry_count += 1
            self.event_writer.write(
                "ingest_retry",
                attempt=attempt,
                max_attempts=attempts,
                batch_docs=batch_size,
                status=status,
                error=str(error) if error else None,
                retryable=retryable,
                body=response_text[:2000] if response_text else "",
            )
            self.capture_logs()
            if not retryable or attempt == attempts:
                break
            time.sleep(self.args.ingest_retry_backoff_seconds * attempt)

        self.state.ingest_failed_docs += batch_size
        self.event_writer.write(
            "ingest_error",
            status=last_status,
            batch_docs=batch_size,
            error=str(last_error) if last_error else None,
            body=last_response_text,
        )
        if last_error is not None:
            raise RuntimeError(
                f"Bulk ingest failed after {attempts} attempts with transport error: {last_error}"
            ) from last_error
        raise RuntimeError(
            f"Bulk ingest failed after {attempts} attempts with HTTP {last_status}: {last_response_text}"
        )

    def ingest_until_target(self) -> None:
        self.set_phase("warm_ingest")
        next_seq = 0
        first_publish_waiting = True
        while self.state.ingest_bytes_sent < self.target_bytes:
            payload, batch_size, latest_seq = self.create_bulk_payload(next_seq)
            status, response_text = self.post_bulk_batch(payload, batch_size)

            bulk_result = parse_ndjson_bulk_response(response_text)
            payload_bytes = len(payload.encode("utf-8"))
            self.state.ingest_bytes_sent += payload_bytes
            self.state.ingest_docs_sent += int(bulk_result["total_docs"])
            self.state.ingest_failed_docs += int(bulk_result["failed_docs"])
            self.state.latest_ingested_seq = latest_seq
            self.event_writer.write(
                "ingest_batch",
                status=status,
                payload_bytes=payload_bytes,
                docs=bulk_result["total_docs"],
                failed_docs=bulk_result["failed_docs"],
                latest_seq=latest_seq,
                total_bytes_sent=self.state.ingest_bytes_sent,
            )

            if first_publish_waiting:
                self.wait_for_first_publish()
                first_publish_waiting = False
                self.set_phase("steady_state_nrt")

            self.run_query_suite()
            self.capture_logs()
            parsed_events = self.parse_all_logs()
            for event in parsed_events:
                if event["type"] == "cache_searchable":
                    self.state.cache_searchable_hosts.add(str(event["host"]))

            self.maybe_restart_indexer()
            next_seq = latest_seq + 1
            time.sleep(self.args.query_cadence)

    def capture_logs(self) -> None:
        services = [
            "astra_preprocessor",
            "astra_index",
            "astra_index_replacement",
            "astra_manager",
            "astra_query",
            "astra_cache_a",
            "astra_cache_b",
            "astra_recovery",
            "kafka",
            "zookeeper",
        ]
        if self.args.storage_backend == "minio":
            services.extend(["minio", "minio_setup"])
        for service in services:
            target = self.logs_dir / f"{service}.log"
            try:
                result = self.compose("logs", "--no-color", service)
                target.write_text(result.stdout, encoding="utf-8")
            except BenchmarkCommandError as exc:
                target.write_text(exc.result.stdout + exc.result.stderr, encoding="utf-8")

    def parse_all_logs(self) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for log_path in sorted(self.logs_dir.glob("*.log")):
            parsed.extend(parse_service_log(log_path.stem, log_path))
        return parsed

    def finalize_summary(self) -> dict[str, Any]:
        self.wait_for_replacement_publish()
        self.capture_logs()
        parsed_events = self.parse_all_logs()
        nrt_events = [event for event in parsed_events if event["type"] == "nrt_publish"]
        cache_events = [event for event in parsed_events if event["type"] == "cache_searchable"]
        s3_upload_errors = [event for event in parsed_events if event["type"] == "s3_upload_error"]
        self.state.cache_searchable_hosts.update(str(event["host"]) for event in cache_events)
        latest_publish = find_latest_publish(parsed_events)
        replacement_publish = find_latest_publish(parsed_events, service="astra_index_replacement")
        self.run_query_suite(final_pass=True)

        latest_visible = self.state.latest_visible_seq if self.state.latest_visible_seq >= 0 else None
        publish_lag_docs = (
            self.state.latest_ingested_seq - latest_visible
            if latest_visible is not None
            else None
        )

        blocked_reasons: list[str] = []
        fail_reasons: list[str] = []

        if not nrt_events:
            blocked_reasons.append("No NRT publish event was observed in indexer logs")
        if s3_upload_errors:
            blocked_reasons.append(
                f"Indexer reported {len(s3_upload_errors)} S3 upload errors before live publication"
            )
        if not cache_events:
            blocked_reasons.append("No cache searchability transition was observed in manager logs")
        if self.state.replacement_started_at and replacement_publish is None:
            blocked_reasons.append("Replacement indexer never published a new NRT generation")

        if self.state.ingest_failed_docs > 0:
            fail_reasons.append(f"Bulk ingest reported {self.state.ingest_failed_docs} failed docs")
        if self.state.query_errors > 0:
            fail_reasons.append(f"Recurring queries recorded {self.state.query_errors} failures")
        if publish_lag_docs is not None and publish_lag_docs > self.lag_budget_docs:
            fail_reasons.append(
                f"Latest visible seq lag {publish_lag_docs} exceeded budget {self.lag_budget_docs}"
            )
        if self.state.replacement_started_at and replacement_publish is not None and latest_publish is not None:
            if int(replacement_publish["generation"]) <= int(self.state.first_publish_generation or 0):
                fail_reasons.append("Replacement indexer did not advance snapshot generation")

        status = "pass"
        if blocked_reasons:
            status = "blocked"
        elif fail_reasons:
            status = "fail"

        summary = {
            "run_id": self.run_id,
            "status": status,
            "tier": self.args.tier,
            "dataset": self.args.dataset,
            "started_at": self.run_started_at,
            "finished_at": iso_now(),
            "target_bytes": self.target_bytes,
            "ingest": {
                "bytes_sent": self.state.ingest_bytes_sent,
                "docs_sent": self.state.ingest_docs_sent,
                "failed_docs": self.state.ingest_failed_docs,
                "retry_count": self.state.ingest_retry_count,
                "latest_ingested_seq": self.state.latest_ingested_seq,
            },
            "queries": {
                "successes": self.state.query_successes,
                "errors": self.state.query_errors,
                "latest_visible_seq": latest_visible,
                "publish_lag_docs": publish_lag_docs,
                "latency_ms": {
                    "count": len(self.state.query_latencies_ms),
                    "p50": percentile(self.state.query_latencies_ms, 0.50),
                    "p95": percentile(self.state.query_latencies_ms, 0.95),
                    "max": max(self.state.query_latencies_ms) if self.state.query_latencies_ms else None,
                },
            },
            "nrt": {
                "first_publish_generation": self.state.first_publish_generation,
                "first_publish_path": self.state.first_publish_path,
                "latest_publish_generation": int(latest_publish["generation"]) if latest_publish else None,
                "replacement_publish_generation": int(replacement_publish["generation"])
                if replacement_publish
                else None,
                "publish_events": len(nrt_events),
                "cache_searchable_hosts": sorted(self.state.cache_searchable_hosts),
            },
            "restart": {
                "trigger_bytes": self.restart_trigger_bytes,
                "indexer_stopped_at": self.state.indexer_stopped_at,
                "replacement_started_at": self.state.replacement_started_at,
                "resume_publish_at": self.state.resume_publish_at,
                "resume_lag_seconds": self.duration_seconds(
                    self.state.replacement_started_at, self.state.resume_publish_at
                ),
                "cache_continuity_gap_seconds": self.duration_seconds(
                    self.state.indexer_stopped_at, self.state.resume_publish_at
                ),
            },
            "s3": {
                "bucket": self.args.s3_bucket,
                "prefix": self.s3_prefix,
                "polls": len(self.state.s3_object_history),
                "latest_manifest_count": len(self.state.s3_object_history[-1]["manifest_keys"])
                if self.state.s3_object_history
                else 0,
            },
            "blocked_reasons": blocked_reasons,
            "fail_reasons": fail_reasons,
        }
        return summary

    def write_report(self, summary: dict[str, Any]) -> None:
        lines = [
            f"# KalDB NRT benchmark report: {self.run_id}",
            "",
            f"- Status: `{summary['status']}`",
            f"- Tier: `{summary['tier']}`",
            f"- Dataset: `{summary['dataset']}`",
            f"- Started: `{summary['started_at']}`",
            f"- Finished: `{summary['finished_at']}`",
            "",
            "## Ingest",
            "",
            f"- Bytes sent: `{summary['ingest']['bytes_sent']}`",
            f"- Docs sent: `{summary['ingest']['docs_sent']}`",
            f"- Failed docs: `{summary['ingest']['failed_docs']}`",
            f"- Ingest retries: `{summary['ingest']['retry_count']}`",
            f"- Latest ingested seq: `{summary['ingest']['latest_ingested_seq']}`",
            "",
            "## Query",
            "",
            f"- Query successes: `{summary['queries']['successes']}`",
            f"- Query errors: `{summary['queries']['errors']}`",
            f"- Latest visible seq: `{summary['queries']['latest_visible_seq']}`",
            f"- Publish lag docs: `{summary['queries']['publish_lag_docs']}`",
            f"- P50 latency ms: `{summary['queries']['latency_ms']['p50']}`",
            f"- P95 latency ms: `{summary['queries']['latency_ms']['p95']}`",
            "",
            "## NRT",
            "",
            f"- First publish generation: `{summary['nrt']['first_publish_generation']}`",
            f"- Latest publish generation: `{summary['nrt']['latest_publish_generation']}`",
            f"- Replacement publish generation: `{summary['nrt']['replacement_publish_generation']}`",
            f"- Cache searchable hosts: `{', '.join(summary['nrt']['cache_searchable_hosts']) or 'none'}`",
            "",
            "## Restart",
            "",
            f"- Indexer stopped at: `{summary['restart']['indexer_stopped_at']}`",
            f"- Replacement started at: `{summary['restart']['replacement_started_at']}`",
            f"- Resume publish observed at: `{summary['restart']['resume_publish_at']}`",
            f"- Resume lag seconds: `{summary['restart']['resume_lag_seconds']}`",
            f"- Cache continuity gap seconds: `{summary['restart']['cache_continuity_gap_seconds']}`",
        ]
        if summary["blocked_reasons"]:
            lines.extend(["", "## Blocked reasons", ""])
            lines.extend([f"- {reason}" for reason in summary["blocked_reasons"]])
        if summary["fail_reasons"]:
            lines.extend(["", "## Fail reasons", ""])
            lines.extend([f"- {reason}" for reason in summary["fail_reasons"]])
        (self.artifact_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def run(self) -> int:
        self.prepare_artifacts()
        collectors: list[threading.Thread] = []
        try:
            self.configure_cluster()
            collectors = self.start_collectors()
            self.ingest_until_target()
            summary = self.finalize_summary()
            json_dump(self.artifact_dir / "summary.json", summary)
            self.write_report(summary)
            return 0 if summary["status"] == "pass" else 2
        finally:
            self.stop_event.set()
            for collector in collectors:
                collector.join(timeout=5)
            try:
                self.capture_logs()
            except Exception:
                self.log(logging.WARNING, "Failed to capture final docker logs")
            if self.args.teardown and self.args.compose_up:
                try:
                    self.compose("down", "-v", "--remove-orphans")
                except Exception:
                    self.log(logging.WARNING, "Failed to tear down compose environment")

    def write_fatal_report(self, exc: Exception) -> None:
        traceback_text = traceback.format_exc()
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "phase": self.current_phase,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback_text,
            "recent_events": self.tail_recent_events(),
        }
        if isinstance(exc, BenchmarkCommandError):
            payload["command"] = {
                "args": exc.result.args,
                "cwd": exc.result.cwd,
                "returncode": exc.result.returncode,
                "started_at": exc.result.started_at,
                "finished_at": exc.result.finished_at,
                "stdout": exc.result.stdout,
                "stderr": exc.result.stderr,
                "label": exc.result.label,
            }
        json_dump(self.artifact_dir / "fatal-error.json", payload)
        lines = [
            f"run_id: {self.run_id}",
            f"phase: {self.current_phase}",
            f"error_type: {type(exc).__name__}",
            f"error: {exc}",
            "",
        ]
        recent_events = payload["recent_events"]
        if recent_events:
            lines.extend(["recent_events:"])
            lines.extend(json.dumps(event, sort_keys=True) for event in recent_events)
            lines.append("")
        if isinstance(exc, BenchmarkCommandError):
            lines.extend(
                [
                    f"command: {' '.join(exc.result.args)}",
                    f"returncode: {exc.result.returncode}",
                    f"cwd: {exc.result.cwd}",
                    "",
                    "stderr:",
                    exc.result.stderr or "<empty>",
                    "",
                    "stdout:",
                    exc.result.stdout or "<empty>",
                    "",
                ]
            )
        lines.extend(["traceback:", traceback_text])
        (self.artifact_dir / "fatal-error.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    """Program entrypoint."""
    args = parse_args()
    harness = BenchmarkHarness(args)

    def handle_signal(signum: int, _: Any) -> None:
        harness.event_writer.write("signal", signum=signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        return harness.run()
    except KeyboardInterrupt:
        harness.event_writer.write("aborted")
        return 130
    except Exception as exc:
        harness.event_writer.write("fatal_error", error=str(exc))
        if harness.logger.handlers:
            harness.log(logging.ERROR, "Fatal benchmark error: %s", exc)
        harness.write_fatal_report(exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
