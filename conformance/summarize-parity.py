#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SDK_FILES = (("Python", "python.json"), ("TypeScript", "typescript.json"), ("Go", "go.json"))


def load_results(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    results: dict[str, dict[str, Any]] = {}
    for result in data.get("results", []):
        case_id = result.get("case_id", result.get("id"))
        if case_id is None:
            continue
        results[str(case_id)] = result
    return results


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: summarize-parity.py <full-parity-artifact-dir>", file=sys.stderr)
        return 2

    artifact_dir = Path(argv[0]).resolve()
    rows: list[dict[str, Any]] = []
    totals = {sdk: {"passed": 0, "failed": 0, "skipped": 0} for sdk, _ in SDK_FILES}

    for contract_dir in sorted(path for path in artifact_dir.iterdir() if path.is_dir()):
        by_sdk = {sdk: load_results(contract_dir / filename) for sdk, filename in SDK_FILES}
        case_ids = sorted({case_id for results in by_sdk.values() for case_id in results})
        for case_id in case_ids:
            available = [results[case_id] for results in by_sdk.values() if case_id in results]
            groups = {str(result.get("group", "")) for result in available}
            coverage_levels = {str(result.get("coverage", "behavior")) for result in available}
            row: dict[str, Any] = {
                "contract": contract_dir.name,
                "case": case_id,
                "group": groups.pop() if len(groups) == 1 else "mixed",
                "coverage": coverage_levels.pop() if len(coverage_levels) == 1 else "mixed",
                "sdks": {},
            }
            for sdk, _ in SDK_FILES:
                result = by_sdk[sdk].get(case_id)
                status = str(result.get("outcome", result.get("status", "missing"))) if result else "missing"
                detail = str(result.get("detail", "result file or case missing")) if result else "result file or case missing"
                row["sdks"][sdk] = {"status": status, "detail": detail}
                bucket = status if status in totals[sdk] else "failed"
                totals[sdk][bucket] += 1
            rows.append(row)

    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    matrix = {"generated_at": generated_at, "totals": totals, "rows": rows}
    (artifact_dir / "matrix.json").write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    symbols = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP", "missing": "MISS"}
    lines = [
        "# SDK component parity matrix",
        "",
        f"Generated: {generated_at}",
        "",
        "| Group | Coverage | Contract | Case | Python | TypeScript | Go |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for row in rows:
        cells = [symbols.get(row["sdks"][sdk]["status"], "FAIL") for sdk, _ in SDK_FILES]
        lines.append(
            f"| {row['group']} | {row['coverage']} | {row['contract']} | {row['case']} | "
            f"{cells[0]} | {cells[1]} | {cells[2]} |"
        )

    lines.extend(["", "## Totals", "", "| SDK | Passed | Failed | Skipped |", "|---|---:|---:|---:|"])
    for sdk, _ in SDK_FILES:
        value = totals[sdk]
        lines.append(f"| {sdk} | {value['passed']} | {value['failed']} | {value['skipped']} |")

    failures = [row for row in rows if any(value["status"] not in {"passed", "skipped"} for value in row["sdks"].values())]
    if failures:
        lines.extend(["", "## Failures", ""])
        for row in failures:
            for sdk, value in row["sdks"].items():
                if value["status"] not in {"passed", "skipped"}:
                    lines.append(f"- `{row['contract']} / {row['case']} / {sdk}`: {value['detail']}")

    (artifact_dir / "matrix.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
