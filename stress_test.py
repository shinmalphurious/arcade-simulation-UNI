"""Standalone CLI Stress Test Suite for the Arcade NFC Credit Simulation Platform.

Usage:
  .venv\\Scripts\\python.exe stress_test.py --requests 2000 --concurrency 100
  .venv\\Scripts\\python.exe stress_test.py --url http://127.0.0.1:8000 --requests 1000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from backend.main import create_app
from backend.models import StressTestRequest
from backend.stress_runner import run_stress_test_against_base_url


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class EmbeddedUvicornServer:
    """Runs the FastAPI app on a real TCP port in a background thread for realistic HTTP stress testing."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.port = port or _find_free_port()
        self.base_url = f"http://{self.host}:{self.port}"
        self._tmp_dir = tempfile.TemporaryDirectory(prefix="arcade_stress_")
        self.app = create_app(data_dir=Path(self._tmp_dir.name), start_simulator=False)
        self.app.state.settings.cloud_latency_ms = 0
        self.app.state.settings.local_latency_ms = 0
        self.app.state.pi_gateway.set_base_url(self.base_url)
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> "EmbeddedUvicornServer":
        self._thread.start()
        deadline = time.time() + 10.0
        while time.time() < deadline:
            try:
                r = httpx.get(f"{self.base_url}/api/sim/snapshot", timeout=1.0)
                if r.status_code == 200:
                    return self
            except Exception:
                time.sleep(0.05)
        raise RuntimeError("Embedded uvicorn server failed to start in time")

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.should_exit = True
        self._thread.join(timeout=5.0)
        self._tmp_dir.cleanup()


def _print_report(title: str, report: dict[str, Any]) -> None:
    lat = report["latency_ms"]
    audit = report["audit"]
    status_str = "PASS" if report["passed"] else "FAIL"
    print("=" * 72)
    print(f"SCENARIO: {title}  [{status_str}]")
    print("=" * 72)
    print(
        f"Players Generated: {report.get('players_generated', report['num_cards'])} | "
        f"Requests: {report['total_requests']} | Concurrency: {report['concurrency']} | "
        f"Mode: {report['target_mode']} | Elapsed: {report['elapsed_sec']}s | "
        f"Throughput: {report['throughput_rps']} req/s"
    )
    print(
        f"Latency (ms): min={lat['min']}  avg={lat['avg']}  p50={lat['p50']}  "
        f"p95={lat['p95']}  p99={lat['p99']}  max={lat['max']}"
    )
    print(f"HTTP 5xx Errors: {report['http_5xx_errors']} | Transport Errors: {report['transport_errors']}")
    print(f"Outcomes: {json.dumps(report['outcomes'])}")
    print(
        f"Invariants: negative_balances={audit['negative_balance_count']}, "
        f"cross_op_leaks={audit['cross_operator_leak_count']}, "
        f"cloud_conserved={audit['cloud']['conserved']}, "
        f"local_conserved={audit['local']['conserved']}"
    )
    print(
        f"  Cloud Ledger: +{audit['cloud']['topped_up']} / -{audit['cloud']['deducted']} "
        f"= {audit['cloud']['remaining_balance']}"
    )
    print(
        f"  Local Ledger: +{audit['local']['topped_up']} / -{audit['local']['deducted']} "
        f"= {audit['local']['remaining_balance']}"
    )


async def run_suite(args: argparse.Namespace) -> int:
    scenarios = [
        (
            "1. Mixed Scale Load (Cloud + Local + Cross-Op + Double-Tap)",
            StressTestRequest(
                total_requests=args.requests,
                concurrency=args.concurrency,
                num_cards=args.cards,
                initial_credits_per_card=args.credits,
                cross_operator_ratio=0.10,
                double_tap_ratio=0.15,
                target_mode=args.mode,
                bypass_debounce=True,
            ),
        ),
        (
            "2. Extreme Hotspot Double-Spend Contention (5 cards, high concurrency)",
            StressTestRequest(
                total_requests=max(200, args.requests // 2),
                concurrency=args.concurrency,
                num_cards=5,
                initial_credits_per_card=15,
                cross_operator_ratio=0.05,
                double_tap_ratio=0.40,
                target_mode=args.mode,
                bypass_debounce=True,
            ),
        ),
    ]

    all_reports: list[dict[str, Any]] = []
    all_passed = True

    if args.url:
        base_url = args.url.rstrip("/")
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as c:
            await c.patch("/api/sim/settings", json={"running": False, "cloud_latency_ms": 0, "local_latency_ms": 0})
        for title, req in scenarios:
            async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as c:
                await c.post("/api/sim/reset")
            rep = await run_stress_test_against_base_url(base_url=base_url, req=req)
            _print_report(title, rep)
            all_reports.append({"title": title, **rep})
            all_passed = all_passed and rep["passed"]
    else:
        for title, req in scenarios:
            with EmbeddedUvicornServer() as srv:
                rep = await run_stress_test_against_base_url(base_url=srv.base_url, req=req)
                _print_report(title, rep)
                all_reports.append({"title": title, **rep})
                all_passed = all_passed and rep["passed"]

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(all_reports, indent=2), encoding="utf-8")
        print(f"\nSaved JSON report to {args.json_out}")

    print("\nFINAL STRESS SUITE RESULT:", "ALL PASSED" if all_passed else "FAILED")
    return 0 if all_passed else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Stress test the Arcade NFC Credit System at scale.")
    parser.add_argument("--url", type=str, default=None, help="Target an already-running server URL (otherwise boots an embedded uvicorn server)")
    parser.add_argument("--requests", type=int, default=1000, help="Total NFC tap requests for Scenario 1 (default: 1000)")
    parser.add_argument("--concurrency", type=int, default=80, help="Max concurrent in-flight HTTP requests (default: 80)")
    parser.add_argument("--cards", type=int, default=50, help="Number of simulated NFC cards (default: 50)")
    parser.add_argument("--credits", type=int, default=30, help="Initial credits per card (default: 30)")
    parser.add_argument("--mode", choices=["mixed", "cloud", "local"], default="mixed", help="Target storage tier")
    parser.add_argument("--json-out", type=str, default=None, help="Optional file path to save JSON results")
    args = parser.parse_args()
    return asyncio.run(run_suite(args))


if __name__ == "__main__":
    raise SystemExit(main())
