"""Concurrency, double-spend race protection, NFC debounce, idempotency, and outage fault-tolerance tests."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from backend.models import StressTestRequest
from backend.stress_runner import run_stress_test_against_base_url


@pytest.mark.asyncio
async def test_concurrent_double_spend_race_protection(arcade_client):
    """50 simultaneous taps on a card with 5 credits on a 3-credit machine must approve strictly 1 tap."""
    client, _ = arcade_client

    card_uid = "04:RA:CE:00:50"
    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-neon-downtown",
            "customer_name": "Racer",
            "initial_credits": 5,
            "card_uid": card_uid,
        },
    )

    # Claw Master (pi-nd-4) costs 3 credits. 50 concurrent requests with bypass_debounce=True
    async def _tap():
        return await client.post(
            "/api/pi/pi-nd-4/tap",
            json={"card_uid": card_uid, "idempotency_key": str(uuid.uuid4()), "bypass_debounce": True},
        )

    responses = await asyncio.gather(*(_tap() for _ in range(50)))
    approved = [r for r in responses if r.status_code == 200 and r.json()["approved"]]
    denied = [r for r in responses if r.status_code == 402]

    assert len(approved) == 1
    assert len(denied) == 49
    assert approved[0].json()["remaining_balance"] == 2

    audit = (await client.get("/api/audit")).json()
    assert audit["passed"] is True
    assert audit["negative_balance_count"] == 0
    assert audit["cloud"]["remaining_balance"] == 2


@pytest.mark.asyncio
async def test_nfc_hardware_debounce_and_server_idempotency(arcade_client):
    client, _ = arcade_client

    card_uid = "04:DB:NC:00:01"
    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-neon-downtown",
            "customer_name": "Debouncer",
            "initial_credits": 10,
            "card_uid": card_uid,
        },
    )

    # First tap succeeds (cost 1 on pi-nd-2)
    r1 = await client.post("/api/pi/pi-nd-2/tap", json={"card_uid": card_uid})
    assert r1.status_code == 200
    assert r1.json()["remaining_balance"] == 9

    # Rapid second tap within 1200ms debounce window is caught by Pi hardware debounce
    r2 = await client.post("/api/pi/pi-nd-2/tap", json={"card_uid": card_uid})
    assert r2.status_code == 429
    assert r2.json()["outcome"] == "debounced"

    # Network retry with identical idempotency_key (bypassing Pi debounce) does NOT double-charge!
    idem = "retry-key-123"
    r3 = await client.post(
        "/api/pi/pi-nd-2/tap",
        json={"card_uid": card_uid, "idempotency_key": idem, "bypass_debounce": True},
    )
    assert r3.status_code == 200
    assert r3.json()["approved"] is True
    assert r3.json()["remaining_balance"] == 8

    r4 = await client.post(
        "/api/pi/pi-nd-2/tap",
        json={"card_uid": card_uid, "idempotency_key": idem, "bypass_debounce": True},
    )
    assert r4.status_code == 200
    assert r4.json()["outcome"] == "duplicate_idempotency"
    assert r4.json()["credits_deducted"] == 0
    assert r4.json()["remaining_balance"] == 8


@pytest.mark.asyncio
async def test_cloud_outage_blocks_cloud_sites_while_local_sites_stay_operational(arcade_client):
    client, _ = arcade_client

    # Seed one cloud card and one local card before outage
    await client.post(
        "/api/kiosk/issue-card",
        json={"site_id": "site-neon-downtown", "initial_credits": 10, "card_uid": "04:CL:00:01"},
    )
    await client.post(
        "/api/kiosk/issue-card",
        json={"site_id": "site-retro-north", "initial_credits": 10, "card_uid": "04:LC:00:01"},
    )

    # Simulate Cloud / WAN Outage
    await client.patch("/api/sim/settings", json={"cloud_outage": True})

    # Cloud tap & kiosk topup fail closed with HTTP 503
    cloud_tap = await client.post("/api/pi/pi-nd-1/tap", json={"card_uid": "04:CL:00:01"})
    assert cloud_tap.status_code == 503
    assert cloud_tap.json()["outcome"] == "server_offline"

    cloud_topup = await client.post(
        "/api/kiosk/topup",
        json={"site_id": "site-neon-downtown", "card_uid": "04:CL:00:01", "credits": 5},
    )
    assert cloud_topup.status_code == 503

    # Local site continues operating normally on LAN!
    local_tap = await client.post("/api/pi/pi-rn-1/tap", json={"card_uid": "04:LC:00:01"})
    assert local_tap.status_code == 200
    assert local_tap.json()["approved"] is True
    assert local_tap.json()["remaining_balance"] == 9

    # Restore Cloud WAN -> Cloud site resumes
    await client.patch("/api/sim/settings", json={"cloud_outage": False})
    cloud_tap_ok = await client.post(
        "/api/pi/pi-nd-1/tap",
        json={"card_uid": "04:CL:00:01", "bypass_debounce": True},
    )
    assert cloud_tap_ok.status_code == 200
    assert cloud_tap_ok.json()["approved"] is True
    assert cloud_tap_ok.json()["remaining_balance"] == 8


@pytest.mark.asyncio
async def test_embedded_scale_stress_suite(arcade_client):
    """Runs 400 concurrent requests across mixed Cloud & Local sites with attacks and checks invariants."""
    _, app = arcade_client
    import httpx

    report = await run_stress_test_against_base_url(
        base_url="http://testserver",
        req=StressTestRequest(
            total_requests=400,
            concurrency=60,
            num_cards=25,
            initial_credits_per_card=20,
            cross_operator_ratio=0.15,
            double_tap_ratio=0.20,
            target_mode="mixed",
            bypass_debounce=True,
        ),
        transport=httpx.ASGITransport(app=app),
    )
    assert report["passed"] is True
    assert report["http_5xx_errors"] == 0
    assert report["transport_errors"] == 0
    assert report["audit"]["negative_balance_count"] == 0
    assert report["audit"]["cross_operator_leak_count"] == 0
    assert report["audit"]["cloud"]["conserved"] is True
    assert report["audit"]["local"]["conserved"] is True
