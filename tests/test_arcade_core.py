"""Core functional tests: card issuance, variable machine costs, operator isolation, and Cloud vs Local sync."""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_cloud_tier_variable_cost_and_same_operator_roaming(arcade_client):
    client, app = arcade_client

    # Issue card at NeonPlay Downtown (Cloud, op-neon) with 10 credits
    issue = await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-neon-downtown",
            "customer_name": "Alice",
            "initial_credits": 10,
            "card_uid": "04:AA:BB:CC:01",
        },
    )
    assert issue.status_code == 200
    assert issue.json()["balance"] == 10

    # Tap VR Mech Arena (pi-nd-3, cost = 4 credits) at Downtown
    tap1 = await client.post("/api/pi/pi-nd-3/tap", json={"card_uid": "04:AA:BB:CC:01"})
    assert tap1.status_code == 200
    b1 = tap1.json()
    assert b1["approved"] is True
    assert b1["credits_deducted"] == 4
    assert b1["remaining_balance"] == 6
    assert b1["storage_mode"] == "cloud"

    # Immediately roam to NeonPlay Westside Mall (same operator op-neon, Cloud)
    # Tap Apex Rally (pi-nm-3, cost = 3 credits) -> must see remaining 6 credits in real time!
    tap2 = await client.post("/api/pi/pi-nm-3/tap", json={"card_uid": "04:AA:BB:CC:01"})
    assert tap2.status_code == 200
    b2 = tap2.json()
    assert b2["approved"] is True
    assert b2["credits_deducted"] == 3
    assert b2["remaining_balance"] == 3

    # Try VR Mech Arena (cost = 4) when only 3 credits remain -> rejected (402)
    tap3 = await client.post(
        "/api/pi/pi-nd-3/tap",
        json={"card_uid": "04:AA:BB:CC:01", "bypass_debounce": True},
    )
    assert tap3.status_code == 402
    assert tap3.json()["approved"] is False
    assert tap3.json()["outcome"] == "insufficient_credits"
    assert tap3.json()["remaining_balance"] == 3

    audit = (await client.get("/api/audit")).json()
    assert audit["passed"] is True
    assert audit["cloud"]["topped_up"] == 10
    assert audit["cloud"]["deducted"] == 7
    assert audit["cloud"]["remaining_balance"] == 3


@pytest.mark.asyncio
async def test_cross_operator_isolation_blocked_cloud_and_local(arcade_client):
    client, _ = arcade_client

    # Issue card on Operator A (op-neon)
    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-neon-downtown",
            "customer_name": "Bob",
            "initial_credits": 20,
            "card_uid": "04:NE:ON:00:01",
        },
    )

    # Attempt to use op-neon card at RetroVault North (op-retro, Local)
    tap_local = await client.post("/api/pi/pi-rn-1/tap", json={"card_uid": "04:NE:ON:00:01"})
    assert tap_local.status_code == 403
    assert tap_local.json()["approved"] is False
    assert tap_local.json()["outcome"] == "operator_mismatch"

    # Attempt to top up op-neon card at op-retro kiosk -> rejected
    topup_rival = await client.post(
        "/api/kiosk/topup",
        json={"site_id": "site-retro-north", "card_uid": "04:NE:ON:00:01", "credits": 5},
    )
    assert topup_rival.status_code == 403

    # Issue card on Operator B (op-retro) and attempt to use at Operator A (op-neon)
    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-retro-north",
            "customer_name": "Cara",
            "initial_credits": 15,
            "card_uid": "04:RE:TR:00:02",
        },
    )
    tap_cloud = await client.post("/api/pi/pi-nd-1/tap", json={"card_uid": "04:RE:TR:00:02"})
    assert tap_cloud.status_code == 403
    assert tap_cloud.json()["approved"] is False
    assert tap_cloud.json()["outcome"] == "operator_mismatch"


@pytest.mark.asyncio
async def test_local_tier_periodic_sync_and_idempotent_reconciliation(arcade_client):
    client, _ = arcade_client

    # Issue card at RetroVault North (Local, op-retro) with 10 credits
    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-retro-north",
            "customer_name": "Dana",
            "initial_credits": 10,
            "card_uid": "04:LO:CA:L0:01",
        },
    )

    # Play 1 game at RetroVault North (Pinball Wizard, cost=2) -> local North balance = 8
    tap_north = await client.post("/api/pi/pi-rn-2/tap", json={"card_uid": "04:LO:CA:L0:01"})
    assert tap_north.status_code == 200
    assert tap_north.json()["remaining_balance"] == 8

    # Before periodic sync runs, RetroVault South does not yet have the card in its isolated local DB
    tap_south_before = await client.post("/api/pi/pi-rs-1/tap", json={"card_uid": "04:LO:CA:L0:01"})
    assert tap_south_before.status_code == 404
    assert tap_south_before.json()["outcome"] == "card_not_found"

    # Trigger periodic sync across same-operator local sites
    sync1 = await client.post("/api/sim/sync-local")
    assert sync1.status_code == 200
    assert sync1.json()["synced"][0]["entries_replicated"] >= 2

    # Triggering sync again must be 100% idempotent (0 new replications, same balance)
    sync2 = await client.post("/api/sim/sync-local")
    assert sync2.status_code == 200
    assert sync2.json()["synced"][0]["entries_replicated"] == 0

    # Now RetroVault South sees the synced 8 credits and lets Dana play Dino Blaster (cost=3) -> 5 left!
    tap_south_after = await client.post("/api/pi/pi-rs-4/tap", json={"card_uid": "04:LO:CA:L0:01"})
    assert tap_south_after.status_code == 200
    assert tap_south_after.json()["approved"] is True
    assert tap_south_after.json()["remaining_balance"] == 5

    # Sync back to North and verify both sites + invariant audit agree on 10 - 2 - 3 = 5
    await client.post("/api/sim/sync-local")
    audit = (await client.get("/api/audit")).json()
    assert audit["passed"] is True
    assert audit["local"]["topped_up"] == 10
    assert audit["local"]["deducted"] == 5
    assert audit["local"]["remaining_balance"] == 5


@pytest.mark.asyncio
async def test_dashboard_metrics_counters_increment_per_outcome(arcade_client):
    """Regression test: approved, insufficient_credits, and operator_mismatch counters must increment in snapshot."""
    client, _ = arcade_client

    await client.post(
        "/api/kiosk/issue-card",
        json={
            "site_id": "site-neon-downtown",
            "customer_name": "MetricTester",
            "initial_credits": 2,
            "card_uid": "04:MT:00:00:01",
        },
    )

    # 1. Approved (cost 2)
    r_ok = await client.post("/api/pi/pi-nd-1/tap", json={"card_uid": "04:MT:00:00:01"})
    assert r_ok.status_code == 200

    # 2. Insufficient credits (0 left)
    r_ins = await client.post(
        "/api/pi/pi-nd-1/tap",
        json={"card_uid": "04:MT:00:00:01", "bypass_debounce": True},
    )
    assert r_ins.status_code == 402

    # 3. Wrong operator (op-neon card tapped at op-retro site)
    r_mis = await client.post("/api/pi/pi-rn-1/tap", json={"card_uid": "04:MT:00:00:01"})
    assert r_mis.status_code == 403

    snap = (await client.get("/api/sim/snapshot")).json()
    m = snap["metrics"]
    assert m["total_taps"] == 3
    assert m["approved"] == 1
    assert m["insufficient_credits"] == 1
    assert m["operator_mismatch"] == 1
