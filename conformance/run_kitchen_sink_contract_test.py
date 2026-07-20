from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

import run_kitchen_sink_contract as contract


class FakeClient:
    def __init__(
        self,
        runs: dict[str, dict[str, Any]],
        events: dict[str, list[str]] | None = None,
        resumes: list[dict[str, Any]] | None = None,
        streams: dict[str, dict[str, Any]] | None = None,
        submissions: dict[str, dict[str, Any]] | None = None,
        cancels: dict[str, dict[str, Any]] | None = None,
        statuses: dict[str, list[dict[str, Any]]] | None = None,
        results: dict[str, dict[str, Any]] | None = None,
        batches: dict[str, dict[str, Any]] | None = None,
        batch_statuses: dict[str, dict[str, Any]] | None = None,
        batch_cancels: dict[str, dict[str, Any]] | None = None,
        chats: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.runs = runs
        self.event_map = events or {}
        self.resumes = resumes or []
        self.streams = streams or {}
        self.submissions = submissions or {}
        self.cancels = cancels or {}
        self.statuses = statuses or {}
        self.results = results or {}
        self.batches = batches or {}
        self.batch_statuses = batch_statuses or {}
        self.batch_cancels = batch_cancels or {}
        self.chats = chats or {}
        self.calls: list[tuple[str, str, Any]] = []
        self.resume_calls: list[tuple[str, Any]] = []
        self.cancel_calls: list[tuple[str, str]] = []
        self.batch_calls: list[tuple[str, str, Any]] = []
        self.batch_status_calls: list[tuple[str, bool]] = []
        self.batch_cancel_calls: list[tuple[str, str]] = []
        self.chat_calls: list[tuple[str, Any]] = []

    def run(self, component_type: str, component: str, payload: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        self.calls.append((component_type, component, payload))
        key = f"{component_type}:{component}"
        if key not in self.runs:
            raise AssertionError(f"unexpected run {key}")
        return 200, self.runs[key]

    def events(self, run_id: str) -> tuple[int, dict[str, Any]]:
        return 200, {"items": [{"event_type": item} for item in self.event_map.get(run_id, [])]}

    def resume(self, run_id: str, user_response: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        self.resume_calls.append((run_id, user_response))
        if not self.resumes:
            raise AssertionError("unexpected resume")
        return 200, self.resumes.pop(0)

    def stream(self, component_type: str, component: str, payload: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        key = f"{component_type}:{component}"
        if key not in self.streams:
            raise AssertionError(f"unexpected stream {key}")
        return 200, self.streams[key]

    def chat(self, component: str, payload: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        self.chat_calls.append((component, payload))
        if component not in self.chats:
            raise AssertionError(f"unexpected chat {component}")
        return 200, self.chats[component]

    def submit(self, component_type: str, component: str, payload: Any, timeout: float) -> tuple[int, dict[str, Any]]:
        key = f"{component_type}:{component}"
        if key not in self.submissions:
            raise AssertionError(f"unexpected submit {key}")
        return 202, self.submissions[key]

    def cancel_run(self, run_id: str, reason: str, timeout: float) -> tuple[int, dict[str, Any]]:
        self.cancel_calls.append((run_id, reason))
        if run_id not in self.cancels:
            raise AssertionError(f"unexpected cancel {run_id}")
        return 200, self.cancels[run_id]

    def status(self, run_id: str) -> tuple[int, dict[str, Any]]:
        values = self.statuses.get(run_id)
        if values:
            if len(values) > 1:
                return 200, values.pop(0)
            return 200, values[0]
        return 200, {"run_id": run_id, "status": "completed"}

    def result(self, run_id: str) -> tuple[int, dict[str, Any]]:
        if run_id not in self.results:
            raise AssertionError(f"unexpected result {run_id}")
        return 200, self.results[run_id]

    def batch(
        self,
        component_type: str,
        component: str,
        items: list[Any],
        metadata: dict[str, str] | None,
        timeout: float,
    ) -> tuple[int, dict[str, Any]]:
        self.batch_calls.append((component_type, component, items))
        key = f"{component_type}:{component}"
        if key not in self.batches:
            raise AssertionError(f"unexpected batch {key}")
        return 202, self.batches[key]

    def batch_status(self, batch_id: str, include_results: bool, timeout: float) -> tuple[int, dict[str, Any]]:
        self.batch_status_calls.append((batch_id, include_results))
        if batch_id not in self.batch_statuses:
            raise AssertionError(f"unexpected batch status {batch_id}")
        data = self.batch_statuses[batch_id]
        return (404 if data.get("status") == "not_found" else 200), data

    def cancel_batch(self, batch_id: str, reason: str, timeout: float) -> tuple[int, dict[str, Any]]:
        self.batch_cancel_calls.append((batch_id, reason))
        if batch_id not in self.batch_cancels:
            raise AssertionError(f"unexpected batch cancel {batch_id}")
        return 200, self.batch_cancels[batch_id]


class KitchenSinkContractTest(unittest.TestCase):
    def test_loads_component_contract(self) -> None:
        data = contract.load_contract(Path(__file__).parent / "contracts/ks_analyze_text.yaml")
        self.assertEqual(data["version"], 1)
        self.assertEqual(data["component"], "ks_analyze_text")
        self.assertGreaterEqual(len(data["cases"]), 1)

    def test_success_output_and_event_order(self) -> None:
        case = {
            "id": "example.success",
            "component_type": "function",
            "component": "ks_analyze_text",
            "input": {"url": "{{gateway_url}}/livez"},
            "expect": {
                "status": "success",
                "output_subset": {"word_count": 3},
                "event_order": ["run.started", "function.completed", "run.completed"],
            },
        }
        client = FakeClient(
            {
                "function:ks_analyze_text": {
                    "run_id": "run-1",
                    "status": "completed",
                    "output": {"word_count": 3, "extra": True},
                }
            },
            {"run-1": ["run.queued", "run.started", "function.started", "function.completed", "run.completed"]},
        )

        result = contract.evaluate_case(
            client,
            case,
            contract.RunnerOptions(gateway_url="http://gw.example"),
        )

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.calls[0][2], {"url": "http://gw.example/livez"})

    def test_failure_error_contains(self) -> None:
        case = {
            "id": "example.failure",
            "component_type": "workflow",
            "component": "ks_error_pipeline",
            "input": {},
            "expect": {"status": "failure", "error_contains": "invalid input"},
        }
        client = FakeClient(
            {
                "workflow:ks_error_pipeline": {
                    "run_id": "run-2",
                    "status": "failed",
                    "error": {"message": "invalid input: bad value"},
                }
            }
        )

        result = contract.evaluate_case(
            client,
            case,
            contract.RunnerOptions(gateway_url="http://gw.example"),
        )

        self.assertTrue(result.passed, result.detail)

    def test_event_order_mismatch_fails(self) -> None:
        case = {
            "id": "example.events",
            "component_type": "function",
            "component": "ks_noop",
            "input": {},
            "expect": {
                "status": "success",
                "event_order": ["run.started", "function.completed", "run.completed"],
            },
        }
        client = FakeClient(
            {"function:ks_noop": {"run_id": "run-3", "status": "completed", "output": {"ok": True}}},
            {"run-3": ["run.started", "run.completed", "function.completed"]},
        )

        result = contract.evaluate_case(
            client,
            case,
            contract.RunnerOptions(gateway_url="http://gw.example"),
        )

        self.assertFalse(result.passed)
        self.assertIn("events missing ordered subsequence", result.detail)

    def test_skip_events_allows_output_only_check(self) -> None:
        case = {
            "id": "example.skip_events",
            "component_type": "function",
            "component": "ks_noop",
            "input": {},
            "expect": {"status": "success", "output_subset": {"ok": True}, "event_order": ["missing"]},
        }
        client = FakeClient(
            {"function:ks_noop": {"run_id": "run-4", "status": "completed", "output": {"ok": True}}}
        )

        result = contract.evaluate_case(
            client,
            case,
            contract.RunnerOptions(gateway_url="http://gw.example", skip_events=True),
        )

        self.assertTrue(result.passed, result.detail)

    def test_paused_resume_flow(self) -> None:
        case = {
            "id": "example.hitl",
            "component_type": "workflow",
            "component": "ks_hitl",
            "input": {},
            "expect": {"status": "paused", "output_subset": {"_paused": True}},
            "resume_steps": [
                {
                    "user_response": "Alice",
                    "expect": {"status": "success", "output_subset": {"name": "Alice"}},
                }
            ],
        }
        client = FakeClient(
            {
                "workflow:ks_hitl": {
                    "run_id": "run-5",
                    "status": "paused",
                    "output": {"_paused": True},
                }
            },
            resumes=[{"run_id": "run-5", "status": "resumed"}],
            statuses={"run-5": [{"run_id": "run-5", "status": "completed"}]},
            results={"run-5": {"run_id": "run-5", "status": "completed", "output": {"name": "Alice"}}},
        )

        result = contract.evaluate_case(
            client,
            case,
            contract.RunnerOptions(gateway_url="http://gw.example"),
        )

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.resume_calls, [("run-5", "Alice")])

    def test_pause_generation_ignores_run_pause_acknowledgement(self) -> None:
        events = {
            "items": [
                {"event_type": "workflow.paused"},
                {"event_type": "run.paused"},
                {"event_type": "workflow.paused"},
                {"event_type": "run.paused"},
            ]
        }

        self.assertEqual(contract.pause_event_count(events), 2)

    def test_stream_operation_checks_sse_order(self) -> None:
        case = {
            "id": "example.stream",
            "operation": "stream",
            "component_type": "function",
            "component": "ks_stream_ticks",
            "input": {"count": 2},
            "expect": {
                "status": "success",
                "event_order": ["output.delta", "output.delta", "run.completed"],
                "stream": {
                    "incremental": True,
                    "terminal_event": "run.completed",
                    "terminal_once": True,
                    "forbid_events_after_terminal": ["output.delta"],
                    "durable_required": ["run.completed"],
                    "durable_forbidden": ["output.delta"],
                    "exact_counts": {"output.delta": 2},
                    "data_sequences": {
                        "output.delta": [
                            {"content": "tick-1"},
                            {"content": "tick-2"},
                        ]
                    },
                    "min_delivery_span_ms": 50,
                    "first_event_max_ms": 150,
                    "terminal_max_ms": 300,
                },
            },
        }
        client = FakeClient(
            {},
            events={"run-stream": ["run.started", "run.completed", "run.archived"]},
            streams={
                "function:ks_stream_ticks": {
                    "run_id": "run-stream",
                    "status": "completed",
                    "events": [
                        {"event_type": "run.started", "_stream": {"received_at_ms": 0}},
                        {
                            "event_type": "output.delta",
                            "data": {"content": "tick-1", "index": 0},
                            "_stream": {"received_at_ms": 100},
                        },
                        {
                            "event_type": "output.delta",
                            "data": {"content": "tick-2", "index": 0},
                            "_stream": {"received_at_ms": 200},
                        },
                        {
                            "event_type": "run.completed",
                            "data": {"output_data": {"emitted": 2}},
                            "_stream": {"received_at_ms": 250},
                        },
                        {"event_type": "run.archived", "_stream": {"received_at_ms": 260}},
                    ],
                }
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(result.event_types[-1], "run.archived")

    def test_stream_operation_rejects_out_of_order_payloads(self) -> None:
        failure = contract.stream_expectation_failure(
            {
                "events": [
                    {"event_type": "output.delta", "data": {"content": "tick-2"}},
                    {"event_type": "output.delta", "data": {"content": "tick-1"}},
                    {"event_type": "run.completed"},
                ]
            },
            {
                "terminal_event": "run.completed",
                "data_sequences": {
                    "output.delta": [
                        {"content": "tick-1"},
                        {"content": "tick-2"},
                    ]
                },
            },
        )

        self.assertIn("payload 0 did not contain expected subset", failure)

    def test_stream_operation_rejects_transient_event_in_durable_history(self) -> None:
        case = {
            "id": "example.durable_delta",
            "operation": "stream",
            "component_type": "function",
            "component": "ks_stream_ticks",
            "input": {"count": 1},
            "expect": {
                "status": "success",
                "stream": {
                    "terminal_event": "run.completed",
                    "durable_required": ["run.completed"],
                    "durable_forbidden": ["output.delta"],
                },
            },
        }
        client = FakeClient(
            {},
            events={"run-durable-delta": ["run.started", "output.delta", "run.completed"]},
            streams={
                "function:ks_stream_ticks": {
                    "run_id": "run-durable-delta",
                    "status": "completed",
                    "events": [
                        {"event_type": "output.delta"},
                        {"event_type": "run.completed"},
                    ],
                }
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertFalse(result.passed)
        self.assertIn("transient events appeared in durable history", result.detail)

    def test_stream_operation_rejects_delta_after_terminal(self) -> None:
        failure = contract.stream_expectation_failure(
            {
                "events": [
                    {"event_type": "output.delta"},
                    {"event_type": "run.failed"},
                    {"event_type": "run.archived"},
                    {"event_type": "output.delta"},
                ]
            },
            {
                "terminal_event": "run.failed",
                "terminal_once": True,
                "forbid_events_after_terminal": ["output.delta"],
            },
        )

        self.assertIn("forbidden events appeared after terminal", failure)

    def test_stream_operation_rejects_buffered_delivery(self) -> None:
        case = {
            "id": "example.buffered_stream",
            "operation": "stream",
            "component_type": "function",
            "component": "ks_stream_ticks",
            "input": {"count": 1},
            "expect": {
                "status": "success",
                "stream": {
                    "incremental": True,
                    "terminal_event": "run.completed",
                    "terminal_once": True,
                },
            },
        }
        client = FakeClient(
            {},
            streams={
                "function:ks_stream_ticks": {
                    "run_id": "run-buffered",
                    "status": "completed",
                    "events": [
                        {"event_type": "output.delta", "_stream": {"received_at_ms": 100}},
                        {"event_type": "run.completed", "_stream": {"received_at_ms": 100}},
                    ],
                }
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertFalse(result.passed)
        self.assertIn("stream was buffered", result.detail)

    def test_stream_operation_rejects_slow_first_delta(self) -> None:
        failure = contract.stream_expectation_failure(
            {
                "events": [
                    {"event_type": "output.delta", "_stream": {"received_at_ms": 250}},
                    {"event_type": "run.completed", "_stream": {"received_at_ms": 300}},
                ]
            },
            {
                "timed_event": "output.delta",
                "terminal_event": "run.completed",
                "first_event_max_ms": 200,
            },
        )

        self.assertIn("arrived at 250ms", failure)

    def test_chat_operation_uses_agent_chat_endpoint_contract(self) -> None:
        case = {
            "id": "example.chat",
            "operation": "chat",
            "component_type": "agent",
            "component": "ks_conversational",
            "input": {"message": "hello", "session_id": "contract"},
            "expect": {"status": "success"},
        }
        client = FakeClient(
            {},
            chats={
                "ks_conversational": {
                    "run_id": "run-chat",
                    "session_id": "contract",
                    "status": "completed",
                    "output": {"output_data": {"response": "hello"}},
                }
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(
            client.chat_calls,
            [("ks_conversational", {"message": "hello", "session_id": "contract"})],
        )

    def test_parse_runtime_sse_frames(self) -> None:
        body = b"""event: output.delta
data: {\"event_type\":\"output.delta\",\"run_id\":\"run-stream\",\"data\":{\"delta\":\"tick-1\"}}

event: run.completed
data: {\"event_type\":\"run.completed\",\"run_id\":\"run-stream\",\"data\":{\"output_data\":{\"emitted\":1}}}

"""

        summary = contract.stream_summary(contract.parse_sse_events(body))

        self.assertEqual(summary["run_id"], "run-stream")
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["output"], {"emitted": 1})
        self.assertEqual(summary["event_types"], ["output.delta", "run.completed"])

    def test_iter_runtime_sse_frames_records_arrival_time(self) -> None:
        lines = [
            b"event: output.delta\n",
            b'data: {"event_type":"output.delta"}\n',
            b"\n",
            b"event: run.completed\n",
            b'data: {"event_type":"run.completed"}\n',
            b"\n",
        ]
        times = iter([10.1, 10.4])

        events = list(contract.iter_sse_events(lines, started_at=10.0, clock=lambda: next(times)))

        self.assertEqual(events[0]["_stream"]["received_at_ms"], 99)
        self.assertEqual(events[1]["_stream"]["received_at_ms"], 400)

    def test_submit_cancel_operation(self) -> None:
        case = {
            "id": "example.cancel",
            "operation": "submit_cancel",
            "component_type": "function",
            "component": "ks_sleep",
            "input": {"duration_ms": 60000},
            "cancel_after_seconds": 0,
            "cancel_reason": "ui-test",
            "expect": {"status": "failure"},
        }
        client = FakeClient(
            {},
            submissions={"function:ks_sleep": {"run_id": "run-cancel", "status": "submitted"}},
            cancels={"run-cancel": {"run_id": "run-cancel", "status": "cancelled"}},
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.cancel_calls, [("run-cancel", "ui-test")])

    def test_batch_operation_waits_for_item_results(self) -> None:
        case = {
            "id": "example.batch",
            "operation": "batch",
            "component_type": "function",
            "component": "ks_noop",
            "items": [{}, {}],
            "expect": {"status": "success", "output_subset": {"stats": {"total_items": 2, "completed_items": 2}}},
        }
        client = FakeClient(
            {},
            batches={"function:ks_noop": {"batch_id": "batch-1", "status": "started", "run_ids": ["run-a", "run-b"]}},
            statuses={
                "run-a": [{"run_id": "run-a", "status": "completed"}],
                "run-b": [{"run_id": "run-b", "status": "completed"}],
            },
            results={
                "run-a": {"run_id": "run-a", "status": "completed", "output": {"ok": True}},
                "run-b": {"run_id": "run-b", "status": "completed", "output": {"ok": True}},
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.batch_calls[0][2], [{}, {}])

    def test_batch_status_operation_queries_runtime_after_items_settle(self) -> None:
        case = {
            "id": "example.batch_status",
            "operation": "batch_status",
            "component_type": "function",
            "component": "ks_noop",
            "items": [{}, {}],
            "expect": {"status": "success", "output_subset": {"stats": {"completed_items": 2}}},
        }
        client = FakeClient(
            {},
            batches={"function:ks_noop": {"batch_id": "batch-2", "status": "started", "run_ids": ["run-c", "run-d"]}},
            statuses={
                "run-c": [{"run_id": "run-c", "status": "completed"}],
                "run-d": [{"run_id": "run-d", "status": "completed"}],
            },
            results={
                "run-c": {"run_id": "run-c", "status": "completed"},
                "run-d": {"run_id": "run-d", "status": "completed"},
            },
            batch_statuses={
                "batch-2": {
                    "batch_id": "batch-2",
                    "status": "completed",
                    "stats": {"total_items": 2, "completed_items": 2},
                }
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.batch_status_calls, [("batch-2", True)])

    def test_batch_status_not_found_is_a_contract_failure_result(self) -> None:
        case = {
            "id": "example.batch_missing",
            "operation": "batch_status",
            "component_type": "function",
            "component": "ks_noop",
            "batch_id": "missing",
            "expect": {"status": "failure", "error_contains": "batch not found"},
        }
        client = FakeClient(
            {},
            batch_statuses={"missing": {"batch_id": "missing", "status": "not_found", "error": "batch not found"}},
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)

    def test_batch_cancel_operation_waits_for_cancelled_children(self) -> None:
        case = {
            "id": "example.batch_cancel",
            "operation": "batch_cancel",
            "component_type": "function",
            "component": "ks_sleep",
            "items": [{"duration_ms": 60000}, {"duration_ms": 60000}],
            "cancel_after_seconds": 0,
            "cancel_reason": "contract",
            "expect": {
                "status": "failure",
                "output_subset": {"stats": {"cancelled_items": 2}},
            },
        }
        client = FakeClient(
            {},
            batches={"function:ks_sleep": {"batch_id": "batch-3", "status": "started", "run_ids": ["run-e", "run-f"]}},
            batch_cancels={"batch-3": {"batch_id": "batch-3", "status": "cancelled", "cancelled_items": 2}},
            statuses={
                "run-e": [{"run_id": "run-e", "status": "cancelled"}],
                "run-f": [{"run_id": "run-f", "status": "cancelled"}],
            },
            results={
                "run-e": {"run_id": "run-e", "status": "cancelled"},
                "run-f": {"run_id": "run-f", "status": "cancelled"},
            },
        )

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.passed, result.detail)
        self.assertEqual(client.batch_cancel_calls, [("batch-3", "contract")])

    def test_required_env_skips_case(self) -> None:
        case = {
            "id": "example.pull",
            "component_type": "function",
            "component": "ks_dispatch_context",
            "input": {},
            "requires_env": "AGNT5_GO_EXPECT_PULL_MODE",
            "expect": {"status": "success"},
        }
        client = FakeClient({})

        result = contract.evaluate_case(client, case, contract.RunnerOptions(gateway_url="http://gw.example"))

        self.assertTrue(result.skipped)


if __name__ == "__main__":
    unittest.main()
