"""Exercise the released Rust transport against a delayed loopback gRPC engine.

This is a diagnostic, not a durable-storage test. The fake engine only
acknowledges Append/AppendBatch; it does not persist or replay records.
Requires agnt5==0.14.8 and grpcio. No platform credentials are used.
"""

import argparse
import asyncio
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version

import grpc
from agnt5._core import PyWorker, PyWorkerConfig


class Engine(grpc.GenericRpcHandler):
    def __init__(self):
        self.lock = threading.Lock()
        self.delay = 0.06
        self.active = self.peak = self.count = 0

    def service(self, details):
        if details.method == "/api.v1.EngineService/Append":
            return grpc.unary_unary_rpc_method_handler(self.append)
        if details.method == "/api.v1.EngineService/AppendBatch":
            return grpc.unary_unary_rpc_method_handler(
                lambda request, context: self.append(request, context, True)
            )
        return None

    @staticmethod
    def varint(value):
        result = bytearray()
        while value > 127:
            result.append((value & 127) | 128)
            value >>= 7
        result.append(value)
        return bytes(result)

    @staticmethod
    def record_count(request):
        def read(offset):
            value = shift = 0
            while True:
                byte = request[offset]
                offset += 1
                value |= (byte & 127) << shift
                if byte < 128:
                    return value, offset
                shift += 7

        count = offset = 0
        while offset < len(request):
            tag, offset = read(offset)
            assert tag == 10, "expected repeated Record field"
            size, offset = read(offset)
            offset += size
            assert offset <= len(request)
            count += 1
        return count

    def append(self, request, context, batch=False):
        count = self.record_count(request) if batch else 1
        with self.lock:
            self.active += 1
            offset = self.count + 1
            self.count += count
            self.groups.append(count)
            self.peak = max(self.peak, self.active)
        time.sleep(self.delay)
        with self.lock:
            self.active -= 1
        if not batch:
            return b"\x08" + self.varint(offset)
        offsets = b"".join(self.varint(i) for i in range(offset, offset + count))
        return (
            b"\x0a" + self.varint(len(offsets)) + offsets + b"\x10" + self.varint(count)
        )


async def sample(worker, engine, mode, repeat, count=11):
    engine.peak = engine.count = 0
    engine.groups = []
    starts = []
    active = peak = 0
    loop_ticks = 0
    stop = False

    async def heartbeat():
        nonlocal loop_ticks
        while not stop:
            loop_ticks += 1
            await asyncio.sleep(0.005)

    async def branch(index):
        nonlocal active, peak
        run_id = f"diagnostic-{mode}-{repeat}-{count}-{engine.delay}" + (
            f"-{index}" if mode == "different_runs" else ""
        )
        await worker.emit_event_async(
            run_id=run_id,
            event_type="function.started",
            event_data=b"{}",
            sequence_number=index + 1,
            metadata={"project_id": "diagnostic-project"},
            source_timestamp_ns=time.time_ns(),
            timeout_ms=5000,
        )
        starts.append(time.monotonic())
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.1)
        active -= 1

    tick = asyncio.create_task(heartbeat())
    begin = time.monotonic()
    try:
        await asyncio.wait_for(
            asyncio.gather(*(branch(i) for i in range(count))),
            max(10, count * engine.delay + 5),
        )
    finally:
        stop = True
        await tick
    assert engine.count == count
    return {
        "mode": mode,
        "repeat": repeat,
        "ack_delay_ms": engine.delay * 1000,
        "checkpoint_calls": engine.count,
        "rpc_batch_sizes": engine.groups,
        "peak_engine_appends": engine.peak,
        "peak_work_concurrency": peak,
        "event_loop_ticks": loop_ticks,
        "work_start_spread_ms": round((max(starts) - min(starts)) * 1000, 3),
        "elapsed_ms": round((time.monotonic() - begin) * 1000, 3),
    }


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--widths", type=int, nargs="+", default=[11])
    parser.add_argument("--delays-ms", type=int, nargs="+", default=[60])
    parser.add_argument("--core-revision", default="published core 0.3.10")
    args = parser.parse_args()
    assert all(1 <= width <= 100 for width in args.widths)
    assert all(0 <= delay <= 500 for delay in args.delays_ms)
    engine = Engine()
    server = grpc.server(ThreadPoolExecutor(max_workers=max(16, max(args.widths))))
    server.add_generic_rpc_handlers((engine,))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    os.environ["AGNT5_ENGINE_URL"] = f"http://127.0.0.1:{port}"
    os.environ["AGNT5_COORDINATOR_ENDPOINT"] = f"http://127.0.0.1:{port}"
    worker = PyWorker(
        PyWorkerConfig(
            "checkpoint-diagnostic", "1", "standalone", max(16, max(args.widths))
        )
    )
    worker.initialize()
    results = []
    try:
        # Establish the channel before taking samples.
        await sample(worker, engine, "different_runs", 0, count=1)
        for delay in args.delays_ms:
            engine.delay = delay / 1000
            for width in args.widths:
                for repeat in range(1, 4):
                    for mode in ("same_run", "different_runs"):
                        results.append(
                            await sample(worker, engine, mode, repeat, count=width)
                        )
    finally:
        server.stop(0).wait()
    print(
        json.dumps(
            {
                "sdk": version("agnt5"),
                "transport": "native PyWorker",
                "core_revision": args.core_revision,
                "engine": "loopback delayed-ack stub, no persistence",
                "samples": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
