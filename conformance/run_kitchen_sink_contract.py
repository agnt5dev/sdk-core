#!/usr/bin/env python3
"""Run an SDK kitchen-sink conformance contract against an AGNT5 gateway."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import yaml
from typing import Any


SUCCESS_STATUSES = {"completed", "success", "succeeded"}
FAILURE_STATUSES = {"failed", "error", "cancelled", "canceled", "timeout", "partial_failure"}
PAUSED_STATUSES = {"paused", "awaiting_input", "awaiting_user_input", "waiting_for_user_input"}
RUNNING_STATUSES = {"pending", "queued", "assigned", "started", "running", "submitted", "resumed"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def compact(value: Any, limit: int = 800) -> str:
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, sort_keys=True)
        except TypeError:
            text = repr(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def parse_json_body(body: bytes) -> dict[str, Any]:
    if not body:
        return {}
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        return {"raw": body.decode("utf-8", errors="replace")}
    return data if isinstance(data, dict) else {"value": data}


def maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return value
    return value


def output_of(data: dict[str, Any]) -> Any:
    if "output" in data:
        return maybe_json(data.get("output"))
    nested = data.get("data")
    if isinstance(nested, dict):
        if "output_data" in nested:
            return maybe_json(nested.get("output_data"))
        if "output" in nested:
            return maybe_json(nested.get("output"))
    result = data.get("result")
    if isinstance(result, dict):
        output = result.get("output")
        if isinstance(output, dict) and "output_data" in output:
            return maybe_json(output.get("output_data"))
        if output is not None:
            return maybe_json(output)
    if any(key in data for key in ("batch_id", "batchId", "stats", "results")):
        return data
    return None


def run_id_of(data: dict[str, Any]) -> str:
    for key in ("run_id", "runId", "id"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    nested = data.get("data")
    if isinstance(nested, dict):
        return run_id_of(nested)
    result = data.get("result")
    if isinstance(result, dict):
        return run_id_of(result)
    return ""


def status_of(data: dict[str, Any]) -> str:
    for key in ("status", "state"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value.lower()
    nested = data.get("data")
    if isinstance(nested, dict):
        return status_of(nested)
    result = data.get("result")
    if isinstance(result, dict):
        return status_of(result)
    return ""


def error_text(data: dict[str, Any]) -> str:
    error = data.get("error")
    if isinstance(error, str):
        return error
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str):
            return message
        return compact(error)
    result = data.get("result")
    if isinstance(result, dict):
        return error_text(result)
    return ""


def is_success(data: dict[str, Any]) -> bool:
    return status_of(data) in SUCCESS_STATUSES and not error_text(data)


def is_failure(data: dict[str, Any]) -> bool:
    return status_of(data) in FAILURE_STATUSES or bool(error_text(data))


def is_paused(data: dict[str, Any]) -> bool:
    if status_of(data) in PAUSED_STATUSES:
        return True
    output = output_of(data)
    return isinstance(output, dict) and output.get("_paused") is True


def expectation_failure(data: dict[str, Any], expect: dict[str, Any]) -> str:
    expected_status = expect.get("status")
    if expected_status == "success" and not is_success(data):
        return f"expected success, got status={status_of(data)!r} error={error_text(data)!r} body={compact(data)}"
    if expected_status == "failure" and not is_failure(data):
        return f"expected failure, got status={status_of(data)!r} body={compact(data)}"
    if expected_status == "paused" and not is_paused(data):
        return f"expected paused, got status={status_of(data)!r} body={compact(data)}"
    if expected_status == "terminal" and not (is_success(data) or is_failure(data) or is_paused(data)):
        return f"expected terminal status, got status={status_of(data)!r} body={compact(data)}"

    expected_error = expect.get("error_contains")
    if isinstance(expected_error, str) and expected_error:
        observed = error_text(data)
        if expected_error.lower() not in observed.lower() and expected_error.lower() not in compact(data).lower():
            return f"expected error containing {expected_error!r}, got {observed!r}"

    expected_output = expect.get("output_subset")
    if expected_output is not None:
        output = output_of(data)
        if not deep_contains(output, expected_output):
            return f"output did not contain expected subset {compact(expected_output)}; got {compact(output)}"

    return ""


def plural_component_type(component_type: str) -> str:
    value = component_type.strip().lower()
    mapping = {
        "workflow": "workflows",
        "function": "functions",
        "agent": "agents",
        "tool": "tools",
        "scorer": "scorers",
        "chat": "chat",
        "mcp": "mcp",
    }
    if value in mapping:
        return mapping[value]
    if value.endswith("s"):
        return value
    return value + "s"


def render_templates(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        out = value
        for key, replacement in context.items():
            out = out.replace("{{" + key + "}}", replacement)
        return out
    if isinstance(value, list):
        return [render_templates(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_templates(item, context) for key, item in value.items()}
    return value


def deep_contains(actual: Any, expected: Any) -> bool:
    actual = maybe_json(actual)
    expected = maybe_json(expected)
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        for key, expected_value in expected.items():
            if key not in actual or not deep_contains(actual[key], expected_value):
                return False
        return True
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) < len(expected):
            return False
        return all(deep_contains(actual[i], expected[i]) for i in range(len(expected)))
    return actual == expected


def event_types_from(data: dict[str, Any]) -> list[str]:
    raw_items = data.get("items")
    if raw_items is None:
        raw_items = data.get("events")
    if not isinstance(raw_items, list):
        return []
    out: list[str] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        for key in ("event_type", "eventType", "type"):
            value = item.get(key)
            if isinstance(value, str) and value:
                out.append(value)
                break
    return out


def terminal_from_events(data: dict[str, Any], run_id: str) -> dict[str, Any] | None:
    raw_events = data.get("events") or data.get("items") or []
    if not isinstance(raw_events, list):
        return None
    for event in reversed(raw_events):
        if not isinstance(event, dict):
            continue
        event_type = event.get("event_type") or event.get("eventType")
        payload = maybe_json(event.get("data"))
        payload = payload if isinstance(payload, dict) else {}
        if event_type == "run.completed":
            return {
                "run_id": run_id,
                "status": "completed",
                "output": payload.get("output_data", payload.get("output")),
            }
        if event_type in {"run.failed", "run.cancelled"}:
            return {
                "run_id": run_id,
                "status": "failed" if event_type == "run.failed" else "cancelled",
                "error": payload.get("error_message") or payload.get("error"),
            }
        if event_type in {"workflow.paused", "run.paused"}:
            return {"run_id": run_id, "status": "paused"}
    return None


def pause_event_count(data: dict[str, Any]) -> int:
    # workflow.paused identifies a new user-input generation. A pull worker
    # may subsequently acknowledge the same generation with run.paused to
    # release its lease; counting both makes that acknowledgement look like
    # the next prompt and causes the runner to resume too early.
    return sum(1 for event_type in event_types_from(data) if event_type == "workflow.paused")


def wait_for_new_pause(client: Any, run_id: str, previous_count: int, timeout: float) -> tuple[dict[str, Any], int]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _, events_data = client.events(run_id)
            count = pause_event_count(events_data)
            if count > previous_count:
                _, status_data = client.status(run_id)
                if status_of(status_data) == "paused":
                    # The journal event arrives before the run projection has
                    # necessarily applied the same pause generation. Give the
                    # processor time to make paused -> resumed dispatch safe.
                    time.sleep(3.0)
                    return {"run_id": run_id, "status": "paused"}, count
            terminal = terminal_from_events(events_data, run_id)
            if terminal is not None and status_of(terminal) != "paused":
                return terminal, count
        except Exception:
            pass
        time.sleep(0.5)
    return {"run_id": run_id, "status": "timeout", "error": {"message": "timed out waiting for next pause"}}, previous_count


def truthy_env(name: str) -> bool:
    value = os.environ.get(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def terminal_status_from_event(event_type: str) -> str:
    mapping = {
        "run.completed": "completed",
        "run.failed": "failed",
        "run.cancelled": "cancelled",
        "workflow.paused": "paused",
        "run.paused": "paused",
        "batch.completed": "completed",
        "batch.cancelled": "cancelled",
    }
    return mapping.get(event_type, "")


def iter_sse_events(
    lines: Any,
    *,
    started_at: float | None = None,
    clock: Any = time.monotonic,
):
    event_name = ""
    data_lines: list[str] = []

    def decoded_event() -> dict[str, Any] | None:
        nonlocal event_name, data_lines
        if not data_lines:
            event_name = ""
            return None
        payload = "\n".join(data_lines)
        parsed = maybe_json(payload)
        if isinstance(parsed, dict):
            event = parsed
            if event_name and "event_type" not in event and "eventType" not in event:
                event["event_type"] = event_name
        else:
            event = {"event_type": event_name or "message", "data": parsed}
        event_name = ""
        data_lines = []
        if started_at is not None:
            event["_stream"] = {"received_at_ms": int((clock() - started_at) * 1000)}
        return event

    for raw_line in lines:
        if isinstance(raw_line, bytes):
            line = raw_line.decode("utf-8", errors="replace")
        else:
            line = str(raw_line)
        line = line.rstrip("\r\n")
        if line == "":
            event = decoded_event()
            if event is not None:
                yield event
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[len("event:") :].strip()
            continue
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].strip())
    event = decoded_event()
    if event is not None:
        yield event


def parse_sse_events(body: bytes) -> list[dict[str, Any]]:
    return list(iter_sse_events(body.splitlines()))


def stream_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    event_types = event_types_from({"events": events})
    run_id = ""
    status = ""
    output = None
    for event in events:
        if not run_id:
            run_id = run_id_of(event)
        event_type = ""
        for key in ("event_type", "eventType", "type"):
            value = event.get(key)
            if isinstance(value, str):
                event_type = value
                break
        terminal = terminal_status_from_event(event_type)
        if terminal:
            status = terminal
            data = event.get("data")
            if isinstance(data, dict):
                output = data.get("output_data", data.get("output"))
            if output is None:
                output = output_of(event)
    return {
        "run_id": run_id,
        "status": status or "unknown",
        "output": output,
        "events": events,
        "event_types": event_types,
    }


def event_type_of(event: dict[str, Any]) -> str:
    for key in ("event_type", "eventType", "type"):
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def event_data_of(event: dict[str, Any]) -> dict[str, Any]:
    data = maybe_json(event.get("data"))
    return data if isinstance(data, dict) else {}


def stream_expectation_failure(data: dict[str, Any], expect: dict[str, Any]) -> str:
    events = data.get("events")
    if not isinstance(events, list):
        return "stream response did not contain events"
    typed_events = [event for event in events if isinstance(event, dict)]
    event_types = [event_type_of(event) for event in typed_events]

    exact_counts = expect.get("exact_counts", {})
    for event_type, expected_count in exact_counts.items():
        observed_count = event_types.count(event_type)
        if observed_count != expected_count:
            return f"expected {expected_count} {event_type} event(s), got {observed_count}: {event_types}"

    data_sequences = expect.get("data_sequences", {})
    if not isinstance(data_sequences, dict):
        return "expect.stream.data_sequences must map event types to expected payload lists"
    for event_type, expected_payloads in data_sequences.items():
        if not isinstance(expected_payloads, list) or not all(
            isinstance(payload, dict) for payload in expected_payloads
        ):
            return f"expect.stream.data_sequences.{event_type} must be a list of object subsets"
        observed_payloads = [
            event_data_of(event)
            for event in typed_events
            if event_type_of(event) == event_type
        ]
        if len(observed_payloads) != len(expected_payloads):
            return (
                f"expected {len(expected_payloads)} payload(s) for {event_type}, "
                f"got {len(observed_payloads)}: {compact(observed_payloads)}"
            )
        for index, (observed, expected_payload) in enumerate(zip(observed_payloads, expected_payloads)):
            if not deep_contains(observed, expected_payload):
                return (
                    f"{event_type} payload {index} did not contain expected subset "
                    f"{compact(expected_payload)}; got {compact(observed)}"
                )

    terminal_event = expect.get("terminal_event")
    terminal_indexes = [
        index
        for index, event_type in enumerate(event_types)
        if terminal_status_from_event(event_type) and (not terminal_event or event_type == terminal_event)
    ]
    if terminal_event and not terminal_indexes:
        return f"expected terminal event {terminal_event!r}, got {event_types}"
    if expect.get("terminal_once") and len(terminal_indexes) != 1:
        return f"expected exactly one terminal event, got {len(terminal_indexes)}: {event_types}"
    if expect.get("no_events_after_terminal") and terminal_indexes and terminal_indexes[-1] != len(event_types) - 1:
        return f"events appeared after terminal event: {event_types}"
    forbidden_after_terminal = expect.get("forbid_events_after_terminal", [])
    if forbidden_after_terminal and terminal_indexes:
        following = event_types[terminal_indexes[0] + 1 :]
        forbidden_seen = [event_type for event_type in following if event_type in forbidden_after_terminal]
        if forbidden_seen:
            return f"forbidden events appeared after terminal event: {forbidden_seen}; got {event_types}"

    timed_event = expect.get("timed_event", "output.delta")
    min_delivery_span_ms = float(expect.get("min_delivery_span_ms", 0))
    if expect.get("incremental") or min_delivery_span_ms > 0:
        timed = [event for event in typed_events if event_type_of(event) == timed_event]
        terminal = typed_events[terminal_indexes[0]] if terminal_indexes else None
        if not timed or terminal is None:
            return f"could not measure incremental delivery for {timed_event!r}: {event_types}"
        first_meta = timed[0].get("_stream")
        terminal_meta = terminal.get("_stream")
        if not isinstance(first_meta, dict) or not isinstance(terminal_meta, dict):
            return "stream events did not include incremental arrival timestamps"
        first_ms = first_meta.get("received_at_ms")
        terminal_ms = terminal_meta.get("received_at_ms")
        if not isinstance(first_ms, int) or not isinstance(terminal_ms, int):
            return "stream arrival timestamps were invalid"
        delivery_span_ms = terminal_ms - first_ms
        if expect.get("incremental") and delivery_span_ms <= 0:
            return f"stream was buffered; {timed_event} and terminal arrived together ({delivery_span_ms}ms span)"
        if delivery_span_ms < min_delivery_span_ms:
            return (
                f"stream delivery span was {delivery_span_ms}ms, "
                f"expected at least {min_delivery_span_ms:g}ms"
            )

    first_event_max_ms = expect.get("first_event_max_ms")
    if first_event_max_ms is not None:
        matching = [event for event in typed_events if event_type_of(event) == timed_event]
        first_meta = matching[0].get("_stream") if matching else None
        first_ms = first_meta.get("received_at_ms") if isinstance(first_meta, dict) else None
        if not isinstance(first_ms, int):
            return f"could not measure first {timed_event!r} delivery: {event_types}"
        if first_ms > float(first_event_max_ms):
            return f"first {timed_event} arrived at {first_ms}ms, expected at most {first_event_max_ms:g}ms"

    terminal_max_ms = expect.get("terminal_max_ms")
    if terminal_max_ms is not None:
        terminal = typed_events[terminal_indexes[0]] if terminal_indexes else None
        terminal_meta = terminal.get("_stream") if terminal else None
        terminal_ms = terminal_meta.get("received_at_ms") if isinstance(terminal_meta, dict) else None
        if not isinstance(terminal_ms, int):
            return f"could not measure terminal delivery: {event_types}"
        if terminal_ms > float(terminal_max_ms):
            return f"terminal event arrived at {terminal_ms}ms, expected at most {terminal_max_ms:g}ms"

    return ""


def contains_ordered(haystack: list[str], needles: list[str]) -> bool:
    pos = 0
    for event_type in haystack:
        if pos < len(needles) and event_type == needles[pos]:
            pos += 1
    return pos == len(needles)


class GatewayClient:
    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        deployment_id: str = "",
        tenant_id: str = "",
        timeout: float = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.deployment_id = deployment_id.strip()
        self.tenant_id = tenant_id.strip()
        self.timeout = timeout

    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        if self.deployment_id:
            headers["X-DEPLOYMENT-ID"] = self.deployment_id
        if self.tenant_id:
            headers["X-TENANT-ID"] = self.tenant_id
        return headers

    def request(
        self,
        method: str,
        path: str,
        payload: Any | None = None,
        timeout: float | None = None,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=self.headers(),
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                body = resp.read()
                return resp.status, parse_json_body(body)
        except urllib.error.HTTPError as exc:
            body = exc.read()
            return exc.code, parse_json_body(body)

    def run(
        self,
        component_type: str,
        component: str,
        payload: Any,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        plural = plural_component_type(component_type)
        return self.request("POST", f"/v1/{plural}/{component}/run", payload, timeout=timeout)

    def submit(
        self,
        component_type: str,
        component: str,
        payload: Any,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        plural = plural_component_type(component_type)
        return self.request("POST", f"/v1/{plural}/{component}/submit", payload, timeout=timeout)

    def stream(
        self,
        component_type: str,
        component: str,
        payload: Any,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        plural = plural_component_type(component_type)
        return self.stream_request("POST", f"/v1/{plural}/{component}/stream", payload, timeout)

    def chat(self, component: str, payload: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        return self.request("POST", f"/v1/agents/{component}/chat", payload, timeout=timeout)

    def batch(
        self,
        component_type: str,
        component: str,
        items: list[Any],
        metadata: dict[str, str] | None,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        plural = plural_component_type(component_type)
        payload: dict[str, Any] = {"items": items}
        if metadata:
            payload["metadata"] = metadata
        return self.request("POST", f"/v1/{plural}/{component}/batch", payload, timeout=timeout)

    def batch_status(
        self,
        batch_id: str,
        include_results: bool,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        encoded_id = urllib.parse.quote(batch_id, safe="")
        query = urllib.parse.urlencode({"include_results": str(include_results).lower()})
        return self.request("GET", f"/v1/batches/{encoded_id}?{query}", timeout=timeout)

    def cancel_batch(
        self,
        batch_id: str,
        reason: str,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        encoded_id = urllib.parse.quote(batch_id, safe="")
        query = urllib.parse.urlencode({"reason": reason}) if reason else ""
        suffix = f"?{query}" if query else ""
        return self.request("DELETE", f"/v1/batches/{encoded_id}{suffix}", timeout=timeout)

    def events(self, run_id: str) -> tuple[int, dict[str, Any]]:
        return self.request("GET", f"/v1/runs/{run_id}/events", timeout=10)

    def status(self, run_id: str) -> tuple[int, dict[str, Any]]:
        return self.request("GET", f"/v1/status/{run_id}", timeout=10)

    def result(self, run_id: str) -> tuple[int, dict[str, Any]]:
        return self.request("GET", f"/v1/result/{run_id}", timeout=10)

    def cancel_run(self, run_id: str, reason: str, timeout: float) -> tuple[int, dict[str, Any]]:
        return self.request("POST", f"/v1/runs/{run_id}/cancel", {"reason": reason}, timeout=timeout)

    def resume(self, run_id: str, user_response: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        return self.request(
            "POST",
            f"/v1/workflows/resume/{run_id}",
            {"user_response": user_response},
            timeout=timeout,
        )

    def stream_request(
        self,
        method: str,
        path: str,
        payload: Any | None,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        headers = self.headers()
        headers["Accept"] = "text/event-stream"
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            started_at = time.monotonic()
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                events = list(iter_sse_events(resp, started_at=started_at))
                return resp.status, stream_summary(events)
        except urllib.error.HTTPError as exc:
            if "text/event-stream" in exc.headers.get("Content-Type", ""):
                events = list(iter_sse_events(exc, started_at=time.monotonic()))
                return exc.code, stream_summary(events)
            body = exc.read()
            return exc.code, parse_json_body(body)


@dataclass
class RunnerOptions:
    gateway_url: str
    skip_events: bool = False


@dataclass
class CaseResult:
    case_id: str
    component_type: str
    component: str
    outcome: str
    detail: str
    duration_ms: int = 0
    http_status: int = 0
    run_id: str = ""
    event_types: list[str] = field(default_factory=list)
    group: str = ""
    coverage: str = "behavior"

    @property
    def passed(self) -> bool:
        return self.outcome == "passed"

    @property
    def skipped(self) -> bool:
        return self.outcome == "skipped"

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "case_id": self.case_id,
            "component_type": self.component_type,
            "component": self.component,
            "outcome": self.outcome,
            "detail": self.detail,
            "duration_ms": self.duration_ms,
        }
        if self.http_status:
            payload["http_status"] = self.http_status
        if self.run_id:
            payload["run_id"] = self.run_id
        if self.event_types:
            payload["event_types"] = self.event_types
        payload["group"] = self.group
        payload["coverage"] = self.coverage
        return payload


def load_contract(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("contract must be a YAML object")
    if data.get("version") != 1:
        raise ValueError("contract version must be 1")
    valid_coverage = {"shape", "behavior", "integration", "resilience"}
    coverage = data.get("coverage", "behavior")
    if coverage not in valid_coverage:
        raise ValueError(f"contract coverage must be one of {sorted(valid_coverage)}")
    group = data.get("group")
    if group is not None and (not isinstance(group, str) or not group):
        raise ValueError("contract group must be a non-empty string")
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("contract must define at least one case")
    seen: set[str] = set()
    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ValueError(f"case {index} must be an object")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id:
            raise ValueError(f"case {index} must define id")
        if case_id in seen:
            raise ValueError(f"duplicate case id {case_id}")
        seen.add(case_id)
        case_coverage = case.get("coverage", coverage)
        if case_coverage not in valid_coverage:
            raise ValueError(f"case {case_id} coverage must be one of {sorted(valid_coverage)}")
        for key in ("component_type", "component", "expect"):
            if key not in case:
                raise ValueError(f"case {case_id} missing {key}")
        expect = case["expect"]
        if not isinstance(expect, dict):
            raise ValueError(f"case {case_id} expect must be an object")
        if expect.get("status") not in {"success", "failure", "paused", "terminal"}:
            raise ValueError(f"case {case_id} has invalid expect.status")
        stream_expect = expect.get("stream")
        if stream_expect is not None:
            if not isinstance(stream_expect, dict):
                raise ValueError(f"case {case_id} expect.stream must be an object")
            if case.get("operation", "run") != "stream":
                raise ValueError(f"case {case_id} expect.stream requires a streaming operation")
            exact_counts = stream_expect.get("exact_counts", {})
            if not isinstance(exact_counts, dict) or any(
                not isinstance(key, str) or not key or not isinstance(value, int) or value < 0
                for key, value in exact_counts.items()
            ):
                raise ValueError(f"case {case_id} expect.stream.exact_counts must map event names to non-negative integers")
            data_sequences = stream_expect.get("data_sequences", {})
            if not isinstance(data_sequences, dict) or any(
                not isinstance(event_type, str)
                or not event_type
                or not isinstance(payloads, list)
                or not all(isinstance(payload, dict) for payload in payloads)
                for event_type, payloads in data_sequences.items()
            ):
                raise ValueError(
                    f"case {case_id} expect.stream.data_sequences must map event names to object lists"
                )
            for key in ("incremental", "terminal_once", "no_events_after_terminal"):
                if key in stream_expect and not isinstance(stream_expect[key], bool):
                    raise ValueError(f"case {case_id} expect.stream.{key} must be a boolean")
            if "terminal_event" in stream_expect and not isinstance(stream_expect["terminal_event"], str):
                raise ValueError(f"case {case_id} expect.stream.terminal_event must be a string")
            if "timed_event" in stream_expect and not isinstance(stream_expect["timed_event"], str):
                raise ValueError(f"case {case_id} expect.stream.timed_event must be a string")
            forbidden_after_terminal = stream_expect.get("forbid_events_after_terminal", [])
            if not isinstance(forbidden_after_terminal, list) or not all(
                isinstance(event_type, str) and event_type for event_type in forbidden_after_terminal
            ):
                raise ValueError(
                    f"case {case_id} expect.stream.forbid_events_after_terminal must be a list of event names"
                )
            for key in ("durable_required", "durable_forbidden"):
                values = stream_expect.get(key, [])
                if not isinstance(values, list) or not all(
                    isinstance(event_type, str) and event_type for event_type in values
                ):
                    raise ValueError(f"case {case_id} expect.stream.{key} must be a list of event names")
            minimum_span = stream_expect.get("min_delivery_span_ms", 0)
            if not isinstance(minimum_span, (int, float)) or minimum_span < 0:
                raise ValueError(f"case {case_id} expect.stream.min_delivery_span_ms must be non-negative")
            for key in ("first_event_max_ms", "terminal_max_ms"):
                maximum = stream_expect.get(key)
                if maximum is not None and (not isinstance(maximum, (int, float)) or maximum <= 0):
                    raise ValueError(f"case {case_id} expect.stream.{key} must be positive")
        events = expect.get("events")
        if events is not None:
            if not isinstance(events, dict):
                raise ValueError(f"case {case_id} expect.events must be an object")
            for key in ("required", "ordered"):
                values = events.get(key, [])
                if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
                    raise ValueError(f"case {case_id} expect.events.{key} must be a list of strings")
            counts = events.get("min_counts", {})
            if not isinstance(counts, dict) or any(not isinstance(value, int) or value < 0 for value in counts.values()):
                raise ValueError(f"case {case_id} expect.events.min_counts must map event names to non-negative integers")
        operation = case.get("operation", "run")
        if operation not in {
            "run",
            "chat",
            "stream",
            "submit_cancel",
            "batch",
            "batch_status",
            "batch_cancel",
        }:
            raise ValueError(f"case {case_id} has invalid operation")
        requires_env = case.get("requires_env")
        if requires_env is not None and not isinstance(requires_env, (str, list)):
            raise ValueError(f"case {case_id} requires_env must be a string or list")
        resume_steps = case.get("resume_steps")
        if resume_steps is not None:
            if not isinstance(resume_steps, list):
                raise ValueError(f"case {case_id} resume_steps must be a list")
            for resume_index, resume_step in enumerate(resume_steps):
                if not isinstance(resume_step, dict):
                    raise ValueError(f"case {case_id} resume step {resume_index} must be an object")
                resume_expect = resume_step.get("expect")
                if resume_expect is not None and (
                    not isinstance(resume_expect, dict)
                    or resume_expect.get("status") not in {"success", "failure", "paused", "terminal"}
                ):
                    raise ValueError(f"case {case_id} resume step {resume_index} has invalid expect.status")
    return data


def required_env_missing(case: dict[str, Any]) -> list[str]:
    requires_env = case.get("requires_env")
    if not requires_env:
        return []
    names = [requires_env] if isinstance(requires_env, str) else list(requires_env)
    return [name for name in names if isinstance(name, str) and not truthy_env(name)]


def wait_for_run(
    client: Any,
    run_id: str,
    timeout: float,
    poll_interval: float = 0.5,
    minimum_wait: float = 0.0,
    ignore_terminal_statuses: set[str] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    ignored = ignore_terminal_statuses or set()
    while time.monotonic() < deadline:
        _, status_data = client.status(run_id)
        latest = status_data
        status = status_of(status_data)
        try:
            _, result_data = client.result(run_id)
        except Exception:
            result_data = {}
        result_status = status_of(result_data)
        settled = time.monotonic() - started >= minimum_wait
        transient_missing = "run not found" in error_text(result_data).lower()
        if settled and result_status and result_status not in RUNNING_STATUSES and result_status not in ignored and not transient_missing:
            return result_data
        if transient_missing:
            if settled:
                try:
                    _, events_data = client.events(run_id)
                    terminal = terminal_from_events(events_data, run_id)
                    if terminal is not None and status_of(terminal) not in ignored:
                        return terminal
                except Exception:
                    pass
            time.sleep(poll_interval)
            continue
        if settled and status and status not in RUNNING_STATUSES and status not in ignored:
            if status_of(result_data):
                return result_data
            merged = dict(result_data)
            merged.setdefault("run_id", run_id)
            merged.setdefault("status", status)
            return merged
        time.sleep(poll_interval)
    latest.setdefault("run_id", run_id)
    try:
        _, events_data = client.events(run_id)
        terminal = terminal_from_events(events_data, run_id)
        if terminal is not None:
            return terminal
    except Exception:
        pass
    latest.setdefault("status", "timeout")
    latest.setdefault("error", {"message": f"timed out waiting for run {run_id}"})
    return latest


def execute_case(client: Any, case: dict[str, Any], options: RunnerOptions) -> tuple[int, dict[str, Any], int, str, list[str]]:
    component_type = str(case["component_type"])
    component = str(case["component"])
    timeout = float(case.get("timeout_seconds") or 30)
    context = {"gateway_url": options.gateway_url.rstrip("/")}
    payload = render_templates(case.get("input", {}), context)
    operation = str(case.get("operation", "run"))

    started = time.monotonic()
    if operation == "stream":
        http_status, data = client.stream(component_type, component, payload, timeout)
    elif operation == "chat":
        http_status, data = client.chat(component, payload, timeout)
    elif operation == "submit_cancel":
        http_status, submitted = client.submit(component_type, component, payload, timeout)
        run_id = run_id_of(submitted)
        if not run_id:
            duration_ms = int((time.monotonic() - started) * 1000)
            return http_status, submitted, duration_ms, "", []
        time.sleep(float(case.get("cancel_after_seconds") or 0.25))
        http_status, data = client.cancel_run(run_id, str(case.get("cancel_reason") or "conformance"), timeout)
        data.setdefault("submitted", submitted)
        data.setdefault("run_id", run_id)
    elif operation in {"batch", "batch_status", "batch_cancel"}:
        raw_items = case.get("items")
        explicit_batch_id = render_templates(case.get("batch_id", ""), context)
        if operation == "batch_status" and isinstance(explicit_batch_id, str) and explicit_batch_id:
            http_status, data = client.batch_status(
                explicit_batch_id,
                bool(case.get("include_results", True)),
                timeout,
            )
        else:
            if not isinstance(raw_items, list):
                raise ValueError(f"case {case['id']} {operation} items must be a list")
            items = render_templates(raw_items, context)
            metadata = render_templates(case.get("metadata", {}), context)
            http_status, submitted = client.batch(component_type, component, items, metadata, timeout)
            batch_id = str(submitted.get("batch_id") or submitted.get("batchId") or "")
            run_ids = submitted.get("run_ids") or submitted.get("runIds") or []
            if operation == "batch_cancel":
                if not batch_id:
                    data = submitted
                else:
                    time.sleep(float(case.get("cancel_after_seconds") or 0.25))
                    http_status, data = client.cancel_batch(
                        batch_id,
                        str(case.get("cancel_reason") or "conformance"),
                        timeout,
                    )
                    data.setdefault("submitted", submitted)
                    data.setdefault("batch_id", batch_id)
                    results = [
                        wait_for_run(client, run_id, timeout)
                        for run_id in run_ids
                        if isinstance(run_id, str) and run_id
                    ] if isinstance(run_ids, list) else []
                    completed = sum(1 for result in results if is_success(result))
                    failed = sum(1 for result in results if status_of(result) == "failed")
                    cancelled = sum(1 for result in results if status_of(result) in {"cancelled", "canceled"})
                    data["results"] = results
                    data["stats"] = {
                        "total_items": len(results),
                        "completed_items": completed,
                        "failed_items": failed,
                        "cancelled_items": cancelled,
                        "pending_items": len(results) - completed - failed - cancelled,
                    }
            else:
                results = [
                    wait_for_run(client, run_id, timeout)
                    for run_id in run_ids
                    if isinstance(run_id, str) and run_id
                ] if isinstance(run_ids, list) else []
                if operation == "batch_status" and batch_id:
                    http_status, data = client.batch_status(
                        batch_id,
                        bool(case.get("include_results", True)),
                        timeout,
                    )
                else:
                    data = submitted
                    completed = sum(1 for result in results if is_success(result))
                    failed = sum(1 for result in results if status_of(result) == "failed")
                    cancelled = sum(1 for result in results if status_of(result) in {"cancelled", "canceled"})
                    data["results"] = results
                    data["stats"] = {
                        "total_items": len(results) or int(data.get("total_items") or data.get("totalItems") or 0),
                        "completed_items": completed,
                        "failed_items": failed,
                        "cancelled_items": cancelled,
                    }
                    if results:
                        data["status"] = (
                            "completed"
                            if completed == len(results)
                            else "partial_failure"
                            if completed
                            else "cancelled"
                            if cancelled == len(results)
                            else "failed"
                        )
    else:
        http_status, data = client.run(component_type, component, payload, timeout)

    duration_ms = int((time.monotonic() - started) * 1000)
    run_id = run_id_of(data)
    event_types = event_types_from(data)
    return http_status, data, duration_ms, run_id, event_types


def evaluate_case(client: Any, case: dict[str, Any], options: RunnerOptions) -> CaseResult:
    case_id = str(case["id"])
    component_type = str(case["component_type"])
    component = str(case["component"])
    expect = case["expect"]
    timeout = float(case.get("timeout_seconds") or 30)

    missing_env = required_env_missing(case)
    if missing_env:
        return CaseResult(
            case_id,
            component_type,
            component,
            "skipped",
            "missing required env: " + ", ".join(missing_env),
        )

    try:
        http_status, data, duration_ms, run_id, event_types = execute_case(client, case, options)
    except Exception as exc:
        return CaseResult(case_id, component_type, component, "failed", f"request failed: {exc}")
    if not run_id:
        run_id = run_id_of(data)

    failure = expectation_failure(data, expect)
    if failure:
        return CaseResult(
            case_id,
            component_type,
            component,
            "failed",
            failure,
            duration_ms,
            http_status,
            run_id,
        )

    stream_expect = expect.get("stream")
    if stream_expect:
        failure = stream_expectation_failure(data, stream_expect)
        if failure:
            return CaseResult(
                case_id,
                component_type,
                component,
                "failed",
                failure,
                duration_ms,
                http_status,
                run_id,
                event_types,
            )
        durable_required = stream_expect.get("durable_required", [])
        durable_forbidden = stream_expect.get("durable_forbidden", [])
        if (durable_required or durable_forbidden) and not options.skip_events:
            if not run_id:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    "response had no run_id for durable stream-event check",
                    duration_ms,
                    http_status,
                )
            try:
                durable_status, durable_data = client.events(run_id)
            except Exception as exc:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"durable event fetch failed: {exc}",
                    duration_ms,
                    http_status,
                    run_id,
                    event_types,
                )
            if durable_status >= 400:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"durable event fetch returned HTTP {durable_status}: {compact(durable_data)}",
                    duration_ms,
                    http_status,
                    run_id,
                    event_types,
                )
            durable_event_types = event_types_from(durable_data)
            missing = [event_type for event_type in durable_required if event_type not in durable_event_types]
            forbidden = [event_type for event_type in durable_forbidden if event_type in durable_event_types]
            if missing:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"durable history missing {missing}; got {durable_event_types}",
                    duration_ms,
                    http_status,
                    run_id,
                    event_types,
                )
            if forbidden:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"transient events appeared in durable history: {forbidden}; got {durable_event_types}",
                    duration_ms,
                    http_status,
                    run_id,
                    event_types,
                )

    event_order = expect.get("event_order")
    if event_order and not options.skip_events:
        if not isinstance(event_order, list) or not all(isinstance(item, str) for item in event_order):
            return CaseResult(case_id, component_type, component, "failed", "expect.event_order must be a string list")
        if not run_id:
            return CaseResult(
                case_id,
                component_type,
                component,
                "failed",
                "response had no run_id for event-order check",
                duration_ms,
                http_status,
            )
        if not event_types:
            try:
                events_status, events_data = client.events(run_id)
            except Exception as exc:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"event fetch failed: {exc}",
                    duration_ms,
                    http_status,
                    run_id,
                )
            if events_status >= 400:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"event fetch returned HTTP {events_status}: {compact(events_data)}",
                    duration_ms,
                    http_status,
                    run_id,
                )
            event_types = event_types_from(events_data)
        if not contains_ordered(event_types, event_order):
            return CaseResult(
                case_id,
                component_type,
                component,
                "failed",
                f"events missing ordered subsequence {event_order}; got {event_types}",
                duration_ms,
                http_status,
                run_id,
                event_types,
            )

    resume_steps = case.get("resume_steps") or []
    if resume_steps:
        if not run_id:
            return CaseResult(
                case_id,
                component_type,
                component,
                "failed",
                "response had no run_id for resume check",
                duration_ms,
                http_status,
                run_id,
                event_types,
            )
        resume_run_id = run_id
        resume_data = data
        resume_http_status = http_status
        try:
            _, initial_events = client.events(resume_run_id)
            observed_pause_count = pause_event_count(initial_events)
        except Exception:
            observed_pause_count = 1 if is_paused(data) else 0
        for index, resume_step in enumerate(resume_steps):
            user_response = render_templates(resume_step.get("user_response", ""), {"gateway_url": options.gateway_url.rstrip("/")})
            resume_timeout = float(resume_step.get("timeout_seconds") or timeout)
            resume_expect = resume_step.get("expect")
            if not resume_expect:
                resume_expect = {"status": "success"} if index == len(resume_steps) - 1 else {"status": "paused"}
            try:
                resume_http_status, resume_data = client.resume(resume_run_id, user_response, resume_timeout)
            except Exception as exc:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"resume step {index + 1} failed: {exc}",
                    duration_ms,
                    http_status,
                    resume_run_id,
                    event_types,
                )
            returned_run_id = run_id_of(resume_data)
            if returned_run_id:
                resume_run_id = returned_run_id
            if resume_expect.get("status") == "paused":
                resume_data, observed_pause_count = wait_for_new_pause(
                    client, resume_run_id, observed_pause_count, resume_timeout
                )
                if status_of(resume_data) == "timeout":
                    try:
                        _, current_status = client.status(resume_run_id)
                        if status_of(current_status) == "paused":
                            resume_http_status, _ = client.resume(resume_run_id, user_response, resume_timeout)
                            resume_data, observed_pause_count = wait_for_new_pause(
                                client, resume_run_id, observed_pause_count, resume_timeout
                            )
                    except Exception:
                        pass
            elif status_of(resume_data) in RUNNING_STATUSES:
                ignored = {"paused"} if resume_expect.get("status") == "success" else set()
                resume_data = wait_for_run(
                    client,
                    resume_run_id,
                    resume_timeout,
                    minimum_wait=2.0,
                    ignore_terminal_statuses=ignored,
                )
                if status_of(resume_data) == "paused":
                    resume_http_status, _ = client.resume(resume_run_id, user_response, resume_timeout)
                    resume_data = wait_for_run(
                        client,
                        resume_run_id,
                        resume_timeout,
                        minimum_wait=2.0,
                        ignore_terminal_statuses={"paused"},
                    )
            failure = expectation_failure(resume_data, resume_expect)
            if failure:
                return CaseResult(
                    case_id,
                    component_type,
                    component,
                    "failed",
                    f"resume step {index + 1}: {failure}",
                    duration_ms,
                    resume_http_status,
                    resume_run_id,
                    event_types,
                )
        return CaseResult(
            case_id,
            component_type,
            component,
            "passed",
            "matched contract with resume",
            duration_ms,
            resume_http_status,
            resume_run_id,
            event_types,
        )

    return CaseResult(
        case_id,
        component_type,
        component,
        "passed",
        "matched contract",
        duration_ms,
        http_status,
        run_id,
        event_types,
    )


def result_summary(contract: dict[str, Any], results: list[CaseResult]) -> dict[str, Any]:
    passed = sum(1 for result in results if result.passed)
    skipped = sum(1 for result in results if result.skipped)
    failed = len(results) - passed - skipped
    return {
        "contract": contract.get("name", ""),
        "sdk": contract.get("sdk", ""),
        "generated_at": utc_now(),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "total": len(results),
        "results": [result.to_json() for result in results],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True, type=Path, help="Path to a YAML conformance contract file")
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("AGNT5_GATEWAY_URL", "http://localhost:34183"),
        help="Gateway base URL",
    )
    parser.add_argument("--api-key", default=os.environ.get("AGNT5_API_KEY", ""), help="Service API key")
    parser.add_argument("--deployment-id", default=os.environ.get("AGNT5_DEPLOYMENT_ID", ""), help="Deployment ID")
    parser.add_argument("--tenant-id", default=os.environ.get("AGNT5_TENANT_ID", ""), help="Tenant ID")
    parser.add_argument("--timeout", type=float, default=30, help="Default HTTP timeout in seconds")
    parser.add_argument("--case", action="append", dest="case_ids", help="Run only a specific case ID")
    parser.add_argument("--list", action="store_true", help="List contract cases and exit")
    parser.add_argument("--skip-events", action="store_true", help="Skip event-order assertions")
    parser.add_argument("--json-output", type=Path, help="Write JSON result summary to this path")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    contract = load_contract(args.contract)
    cases = contract["cases"]
    if args.case_ids:
        selected = set(args.case_ids)
        cases = [case for case in cases if case["id"] in selected]
        missing = selected - {case["id"] for case in cases}
        if missing:
            print(f"unknown case id(s): {', '.join(sorted(missing))}", file=sys.stderr)
            return 2
    if args.list:
        for case in cases:
            print(f"{case['id']}\t{case['component_type']}\t{case['component']}")
        return 0

    client = GatewayClient(
        args.gateway_url,
        api_key=args.api_key,
        deployment_id=args.deployment_id,
        tenant_id=args.tenant_id,
        timeout=args.timeout,
    )
    options = RunnerOptions(gateway_url=args.gateway_url, skip_events=args.skip_events)
    results = [evaluate_case(client, case, options) for case in cases]
    for case, result in zip(cases, results):
        result.group = str(case.get("group") or contract.get("group") or case["component_type"])
        result.coverage = str(case.get("coverage") or contract.get("coverage") or "behavior")
    summary = result_summary(contract, results)

    for result in results:
        marker = "SKIP" if result.skipped else "PASS" if result.passed else "FAIL"
        print(f"{marker} {result.case_id}: {result.detail}")
    print(f"{summary['passed']}/{summary['total']} passed, {summary['skipped']} skipped")

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        with args.json_output.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
