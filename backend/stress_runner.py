"""High-concurrency async stress-test engine with financial invariant verification."""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from typing import Any

import httpx

from backend.config import DEFAULT_MACHINES, DEFAULT_SITES
from backend.models import StorageMode, StressTestRequest


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(len(sorted_vals) * pct)))
    return round(sorted_vals[idx], 2)


async def run_stress_test_against_base_url(
    base_url: str,
    req: StressTestRequest,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    """Executes a full concurrent load + race-condition stress test over real HTTP endpoints."""
    client_kwargs: dict[str, Any] = {
        "base_url": base_url.rstrip("/"),
        "timeout": httpx.Timeout(30.0),
    }
    if transport is not None:
        client_kwargs["transport"] = transport
    else:
        client_kwargs["limits"] = httpx.Limits(
            max_connections=max(100, req.concurrency * 2),
            max_keepalive_connections=max(50, req.concurrency),
        )

    async with httpx.AsyncClient(**client_kwargs) as client:
        # Filter target sites & machines based on mode
        if req.target_mode == "cloud":
            target_sites = [s for s in DEFAULT_SITES if s.storage_mode == StorageMode.CLOUD]
        elif req.target_mode == "local":
            target_sites = [s for s in DEFAULT_SITES if s.storage_mode == StorageMode.LOCAL]
        else:
            target_sites = list(DEFAULT_SITES)

        target_site_ids = {s.id for s in target_sites}
        machines_by_site: dict[str, list[dict[str, Any]]] = {}
        for m in DEFAULT_MACHINES:
            if m.site_id in target_site_ids:
                machines_by_site.setdefault(m.site_id, []).append(m.model_dump())

        all_machines_by_op: dict[str, list[dict[str, Any]]] = {}
        site_op_map = {s.id: s.operator_id for s in DEFAULT_SITES}
        for m in DEFAULT_MACHINES:
            op_id = site_op_map[m.site_id]
            all_machines_by_op.setdefault(op_id, []).append(m.model_dump())

        # 1. Seed test cards via Kiosk API
        seeded_cards: list[dict[str, str]] = []
        seed_sem = asyncio.Semaphore(min(40, req.concurrency))

        async def _seed_one(idx: int) -> None:
            site = target_sites[idx % len(target_sites)]
            card_uid = f"ST:{uuid.uuid4().hex[:10].upper()}"
            async with seed_sem:
                r = await client.post(
                    "/api/kiosk/issue-card",
                    json={
                        "site_id": site.id,
                        "customer_name": f"StressPlayer-{idx}",
                        "initial_credits": req.initial_credits_per_card,
                        "card_uid": card_uid,
                    },
                )
                r.raise_for_status()
            seeded_cards.append(
                {
                    "card_uid": card_uid,
                    "home_site_id": site.id,
                    "operator_id": site.operator_id,
                    "storage_mode": site.storage_mode.value,
                }
            )

        await asyncio.gather(*(_seed_one(i) for i in range(req.num_cards)))

        # If testing local mode, trigger sync so cards are visible across same-operator local sites too
        if req.target_mode in ("local", "mixed"):
            await client.post("/api/sim/sync-local")

        # 2. Build request workload
        workload: list[tuple[str, str, str]] = []  # (pi_id, card_uid, tag)
        for _ in range(req.total_requests):
            card = random.choice(seeded_cards)
            roll = random.random()
            if roll < req.cross_operator_ratio:
                # Pick a rival operator machine
                rival_ops = [op for op in all_machines_by_op if op != card["operator_id"]]
                if rival_ops:
                    rival_machine = random.choice(all_machines_by_op[random.choice(rival_ops)])
                    workload.append((rival_machine["pi_id"], card["card_uid"], "cross_op"))
                    continue

            # Cloud cards can roam across any same-operator Cloud site simultaneously;
            # Local-tier cards are physically played at their home site between periodic sync windows.
            if card["storage_mode"] == "local":
                eligible_machines = machines_by_site[card["home_site_id"]]
            else:
                eligible_machines = [
                    m for m in all_machines_by_op[card["operator_id"]]
                    if m["site_id"] in target_site_ids
                ]
            chosen_m = random.choice(eligible_machines)
            workload.append((chosen_m["pi_id"], card["card_uid"], "normal"))

        # Inject explicit simultaneous double-tap pairs at the same site
        double_pairs = int(req.total_requests * req.double_tap_ratio // 2)
        for i in range(min(double_pairs, len(workload) // 2)):
            card = random.choice(seeded_cards)
            if card["storage_mode"] == "local":
                eligible_machines = machines_by_site[card["home_site_id"]]
            else:
                eligible_machines = [
                    m for m in all_machines_by_op[card["operator_id"]]
                    if m["site_id"] in target_site_ids
                ]
            if len(eligible_machines) >= 2:
                m1, m2 = random.sample(eligible_machines, 2)
                workload[i * 2] = (m1["pi_id"], card["card_uid"], "double_tap")
                workload[i * 2 + 1] = (m2["pi_id"], card["card_uid"], "double_tap")

        # 3. Execute concurrent stress test
        sem = asyncio.Semaphore(req.concurrency)
        latencies_ms: list[float] = []
        outcomes: dict[str, int] = {}
        http_5xx = 0
        transport_errors = 0

        async def _fire(pi_id: str, card_uid: str) -> None:
            nonlocal http_5xx, transport_errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    resp = await client.post(
                        f"/api/pi/{pi_id}/tap",
                        json={
                            "card_uid": card_uid,
                            "idempotency_key": str(uuid.uuid4()),
                            "bypass_debounce": req.bypass_debounce,
                        },
                    )
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    latencies_ms.append(dt_ms)
                    if resp.status_code >= 500 and resp.status_code != 503:
                        http_5xx += 1
                    data = resp.json()
                    outcome = str(data.get("outcome", f"http_{resp.status_code}"))
                    outcomes[outcome] = outcomes.get(outcome, 0) + 1
                except Exception:
                    transport_errors += 1

        t_start = time.perf_counter()
        await asyncio.gather(*(_fire(pi_id, uid) for pi_id, uid, _ in workload))
        total_elapsed_sec = max(0.001, time.perf_counter() - t_start)

        # 4. Sync local sites and audit strict financial invariants
        await client.post("/api/sim/sync-local")
        audit_resp = await client.get("/api/audit")
        audit_resp.raise_for_status()
        audit = audit_resp.json()

        sorted_lats = sorted(latencies_ms)
        throughput_rps = round(len(workload) / total_elapsed_sec, 2)

        return {
            "total_requests": len(workload),
            "concurrency": req.concurrency,
            "num_cards": req.num_cards,
            "players_generated": len(seeded_cards),
            "target_mode": req.target_mode,
            "elapsed_sec": round(total_elapsed_sec, 3),
            "throughput_rps": throughput_rps,
            "http_5xx_errors": http_5xx,
            "transport_errors": transport_errors,
            "outcomes": outcomes,
            "latency_ms": {
                "min": round(sorted_lats[0], 2) if sorted_lats else 0.0,
                "avg": round(sum(sorted_lats) / len(sorted_lats), 2) if sorted_lats else 0.0,
                "p50": _percentile(sorted_lats, 0.50),
                "p95": _percentile(sorted_lats, 0.95),
                "p99": _percentile(sorted_lats, 0.99),
                "max": round(sorted_lats[-1], 2) if sorted_lats else 0.0,
            },
            "audit": audit,
            "passed": bool(audit.get("passed")) and http_5xx == 0 and transport_errors == 0,
        }
