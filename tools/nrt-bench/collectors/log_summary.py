#!/usr/bin/env python3
"""Parses benchmark-relevant KalDB log events from captured service logs."""

from __future__ import annotations

import re
from pathlib import Path


INDEX_PUBLISH_RE = re.compile(
    r"Published NRT snapshot partition=(?P<partition>\S+) snapshot=(?P<snapshot>\S+) "
    r"generation=(?P<generation>\d+) path=(?P<path>\S+) offset=(?P<offset>\d+)"
)
CACHE_SEARCHABLE_RE = re.compile(r"Marking cache node (?P<host>\S+) as searchable")
LIVE_UPDATE_FAILURE_RE = re.compile(
    r"Failed to apply live snapshot update snapshot=(?P<snapshot>\S+) generation=(?P<generation>\d+)"
)
LIVE_ASSIGNMENT_ERROR_RE = re.compile(r"Error handling live chunk assignment")
S3_UPLOAD_ERROR_RE = re.compile(r"Error attempting to upload file to S3")


def parse_service_log(service: str, log_path: Path) -> list[dict[str, object]]:
    """Returns structured events extracted from one captured service log."""
    events: list[dict[str, object]] = []
    if not log_path.exists():
        return events

    for line_number, line in enumerate(
        log_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
    ):
        publish_match = INDEX_PUBLISH_RE.search(line)
        if publish_match:
            events.append(
                {
                    "service": service,
                    "type": "nrt_publish",
                    "line": line_number,
                    "partition": publish_match.group("partition"),
                    "snapshot": publish_match.group("snapshot"),
                    "generation": int(publish_match.group("generation")),
                    "path": publish_match.group("path"),
                    "offset": int(publish_match.group("offset")),
                    "message": line,
                }
            )
            continue

        cache_match = CACHE_SEARCHABLE_RE.search(line)
        if cache_match:
            events.append(
                {
                    "service": service,
                    "type": "cache_searchable",
                    "line": line_number,
                    "host": cache_match.group("host"),
                    "message": line,
                }
            )
            continue

        update_failure = LIVE_UPDATE_FAILURE_RE.search(line)
        if update_failure:
            events.append(
                {
                    "service": service,
                    "type": "live_update_failure",
                    "line": line_number,
                    "snapshot": update_failure.group("snapshot"),
                    "generation": int(update_failure.group("generation")),
                    "message": line,
                }
            )
            continue

        if LIVE_ASSIGNMENT_ERROR_RE.search(line):
            events.append(
                {
                    "service": service,
                    "type": "live_assignment_error",
                    "line": line_number,
                    "message": line,
                }
            )
            continue

        if S3_UPLOAD_ERROR_RE.search(line):
            events.append(
                {
                    "service": service,
                    "type": "s3_upload_error",
                    "line": line_number,
                    "message": line,
                }
            )

    return events
