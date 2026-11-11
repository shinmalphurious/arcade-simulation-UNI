"""Autonomous customer simulation engine, periodic local-site sync scheduler, and live telemetry state."""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

from backend.config import DEFAULT_MACHINES, DEFAULT_OPERATORS, DEFAULT_SITES
from backend.database import ArcadeDatabaseManager
from backend.models import (
    MachineStatus,
    OperatorConfig,
    SimulationSettings,
    SiteConfig,
    StorageMode,
    TapOutcome,
)
from backend.pi_gateway import PiGateway


SITE_POSITIONS = {
    "kiosk": (0.16, 0.54),
    "entrance": (0.16, 0.88),
    "server": (0.16, 0.20),
    "slots": [
        (0.45, 0.34),
        (0.77, 0.34),
        (0.45, 0.72),
        (0.77, 0.72),
    ],
}

CUSTOMER_NAMES = [
    "Maya", "Liam", "Noah", "Aria", "Zoe", "Ethan", "Kai", "Chloe",
    "Lucas", "Nora", "Leo", "Mila", "Owen", "Ruby", "Ezra", "Ava",
]


@dataclass
class SimulatedCustomer:
    id: str
    name: str
    card_uid: str
    operator_id: str
    site_id: str
    behavior: str = "normal"
    x: float = 0.16
    y: float = 0.88
    target_x: float = 0.16
    target_y: float = 0.54
    state: str = "walking_kiosk"  # walking_kiosk | walking_machine | playing | roaming | leaving
    target_machine_id: Optional[str] = None
    initial_credits: int = 12
    has_card_issued: bool = False
    known_balance: int = 0
    games_played: int = 0
    max_games: int = 4
    wait_until: float = 0.0
    roamed_once: bool = False
    color: str = "#a78bfa"
    bubble_text: Optional[str] = None
    bubble_color: str = "#38bdf8"
    bubble_until: float = 0.0


class ArcadeSimulationEngine:
    """Drives autonomous customers, local periodic sync, and real-time event streaming."""

    def __init__(
        self,
        db: ArcadeDatabaseManager,
        pi_gateway: PiGateway,
        settings: SimulationSettings,
        operators: list[OperatorConfig] | None = None,
        sites: list[SiteConfig] | None = None,
    ) -> None:
        self.db = db
        self.pi_gateway = pi_gateway
        self.settings = settings
        self.operators = {op.id: op for op in (operators or DEFAULT_OPERATORS)}
        self.sites = {s.id: s for s in (sites or DEFAULT_SITES)}
        self.customers: dict[str, SimulatedCustomer] = {}
        self.event_log: deque[dict[str, Any]] = deque(maxlen=120)
        self.latencies_ms: deque[float] = deque(maxlen=250)
        self.ws_clients: set[Any] = set()

        # Telemetry metrics
        self.metrics: dict[str, int] = {
            "total_taps": 0,
            "approved": 0,
            "insufficient_credits": 0,
            "operator_mismatch": 0,
            "card_not_found": 0,
            "debounced": 0,
            "duplicate_idempotency": 0,
            "server_offline": 0,
            "topups": 0,
            "sync_runs": 0,
        }

        self._loop_task: Optional[asyncio.Task[Any]] = None
        self._stop_event = asyncio.Event()
        self._last_auto_spawn = 0.0

        # Hook PiGateway events into telemetry & websocket broadcast
        self.pi_gateway.on_event = self._handle_pi_event

    def start(self) -> None:
        if self._loop_task is None or self._loop_task.done():
            self._stop_event.clear()
            self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

    async def reset(self) -> None:
        self.customers.clear()
        self.event_log.clear()
        self.latencies_ms.clear()
        for k in self.metrics:
            self.metrics[k] = 0
        self.pi_gateway.reset_runtime_states()
        await self.db.reset()
        await self._broadcast({"type": "reset", "snapshot": await self.get_snapshot()})

    def _generate_card_uid(self) -> str:
        return ":".join(f"{random.randint(0, 255):02X}" for _ in range(5))

    def spawn_customer(
        self,
        site_id: Optional[str] = None,
        customer_name: Optional[str] = None,
        initial_credits: int = 12,
        behavior: str = "normal",
    ) -> SimulatedCustomer:
        chosen_site_id = site_id or random.choice(list(self.sites.keys()))
        site = self.sites[chosen_site_id]
        op = self.operators[site.operator_id]

        cust_id = f"cust-{uuid.uuid4().hex[:8]}"
        name = customer_name or f"{random.choice(CUSTOMER_NAMES)} #{random.randint(10, 99)}"
        kiosk_x, kiosk_y = SITE_POSITIONS["kiosk"]
        ent_x, ent_y = SITE_POSITIONS["entrance"]

        cust = SimulatedCustomer(
            id=cust_id,
            name=name,
            card_uid=self._generate_card_uid(),
            operator_id=op.id,
            site_id=chosen_site_id,
            behavior=behavior,
            x=ent_x + random.uniform(-0.04, 0.04),
            y=ent_y,
            target_x=kiosk_x,
            target_y=kiosk_y,
            state="walking_kiosk",
            initial_credits=initial_credits,
            max_games=random.randint(3, 5),
            color=op.color,
        )
        self.customers[cust_id] = cust
        return cust

    async def _handle_pi_event(self, event: dict[str, Any]) -> None:
        self.metrics["total_taps"] += 1
        raw_outcome = event.get("outcome", "")
        outcome = raw_outcome.value if isinstance(raw_outcome, TapOutcome) else str(raw_outcome)
        if outcome in self.metrics:
            self.metrics[outcome] += 1
        latency = float(event.get("latency_ms", 0.0))
        if latency > 0:
            self.latencies_ms.append(latency)
        self.event_log.appendleft(event)
        await self._broadcast(event)

    async def trigger_local_sync(self, manual: bool = False) -> list[dict[str, Any]]:
        if self.settings.cloud_outage and not manual:
            return []
        local_ops = {
            s.operator_id
            for s in self.sites.values()
            if s.storage_mode == StorageMode.LOCAL
        }
        results = []
        for op_id in local_ops:
            res = await self.db.sync_local_sites_for_operator(op_id)
            results.append(res)
            self.metrics["sync_runs"] += 1
            sync_evt = {
                "type": "sync_event",
                "timestamp": time.time(),
                **res,
            }
            self.event_log.appendleft(sync_evt)
            await self._broadcast(sync_evt)
        return results

    async def _run_loop(self) -> None:
        # Seed initial customers so canvas immediately looks alive
        if not self.customers:
            self.spawn_customer(site_id="site-neon-downtown", initial_credits=12, behavior="roam_same_op")
            self.spawn_customer(site_id="site-neon-mall", initial_credits=10, behavior="normal")
            self.spawn_customer(site_id="site-retro-north", initial_credits=10, behavior="roam_same_op")
            self.spawn_customer(site_id="site-retro-south", initial_credits=8, behavior="cross_op_attempt")

        last_tick = time.time()
        last_broadcast = 0.0

        while not self._stop_event.is_set():
            now = time.time()
            dt = min(now - last_tick, 0.25)
            last_tick = now

            self.pi_gateway.tick_machines(now)

            if self.settings.running:
                # 1. Auto-maintain a lively arcade population (6-9 active customers)
                if len(self.customers) < 7 and (now - self._last_auto_spawn) > (2.0 / self.settings.speed):
                    self._last_auto_spawn = now
                    behavior = random.choices(
                        ["normal", "roam_same_op", "cross_op_attempt", "double_tap"],
                        weights=[0.50, 0.25, 0.15, 0.10],
                    )[0]
                    self.spawn_customer(
                        initial_credits=random.choice([8, 10, 14, 18]),
                        behavior=behavior,
                    )

                # 2. Step customer movement & state machines
                await self._step_customers(dt, now)

                # 3. Check periodic sync for Local-tier sites
                for sid, last_sync in list(self.db.last_sync_at.items()):
                    if (now - last_sync) >= self.settings.local_sync_interval_sec:
                        await self.trigger_local_sync(manual=False)
                        break

            # 4. Broadcast live snapshot ~10 times/sec
            if (now - last_broadcast) >= 0.10:
                last_broadcast = now
                await self._broadcast({"type": "snapshot", "snapshot": await self.get_snapshot()})

            await asyncio.sleep(0.05)

    async def _step_customers(self, dt: float, now: float) -> None:
        move_speed = 0.24 * self.settings.speed
        to_remove: list[str] = []

        for cust in list(self.customers.values()):
            if now < cust.wait_until:
                continue

            dx = cust.target_x - cust.x
            dy = cust.target_y - cust.y
            dist = (dx * dx + dy * dy) ** 0.5

            if dist > 0.015:
                step = min(dist, move_speed * dt)
                cust.x += (dx / dist) * step
                cust.y += (dy / dist) * step
                continue

            # Arrived at target!
            cust.x = cust.target_x
            cust.y = cust.target_y

            if cust.state == "walking_kiosk":
                await self._on_arrive_kiosk(cust, now)
            elif cust.state == "walking_machine":
                await self._on_arrive_machine(cust, now)
            elif cust.state == "playing":
                await self._on_finish_play(cust, now)
            elif cust.state == "leaving":
                to_remove.append(cust.id)

        for cid in to_remove:
            self.customers.pop(cid, None)

    async def _on_arrive_kiosk(self, cust: SimulatedCustomer, now: float) -> None:
        site = self.sites[cust.site_id]
        # Cloud sites cannot process new cloud kiosk top-ups during a cloud outage
        if site.storage_mode == StorageMode.CLOUD and self.settings.cloud_outage:
            cust.bubble_text = "CLOUD OFFLINE"
            cust.bubble_color = "#ef4444"
            cust.bubble_until = now + 2.0
            cust.wait_until = now + 1.5
            return

        if not cust.has_card_issued:
            res = await self.db.issue_or_topup_card(
                site_id=cust.site_id,
                card_uid=cust.card_uid,
                customer_name=cust.name,
                credits=cust.initial_credits,
                tx_type="issue",
            )
            cust.has_card_issued = True
            cust.known_balance = int(res["balance"])
            cust.bubble_text = f"+{cust.initial_credits} CR (NEW CARD)"
        else:
            res = await self.db.issue_or_topup_card(
                site_id=cust.site_id,
                card_uid=cust.card_uid,
                customer_name=cust.name,
                credits=8,
                tx_type="topup",
            )
            cust.known_balance = int(res["balance"])
            cust.bubble_text = "+8 CR TOP-UP"

        self.metrics["topups"] += 1
        cust.bubble_color = "#22c55e"
        cust.bubble_until = now + 2.2
        topup_evt = {
            "type": "topup_event",
            "timestamp": now,
            "site_id": cust.site_id,
            "operator_id": cust.operator_id,
            "card_uid": cust.card_uid,
            "customer_name": cust.name,
            "balance": cust.known_balance,
            "storage_mode": site.storage_mode.value,
        }
        self.event_log.appendleft(topup_evt)
        await self._broadcast(topup_evt)

        self._pick_next_machine(cust, now)

    def _pick_next_machine(self, cust: SimulatedCustomer, now: float) -> None:
        site_machines = [
            m for m in self.pi_gateway.machines.values()
            if m.config.site_id == cust.site_id
        ]
        idle_machines = [m for m in site_machines if m.status == MachineStatus.IDLE]
        chosen = random.choice(idle_machines or site_machines)
        cust.target_machine_id = chosen.config.id
        sx, sy = SITE_POSITIONS["slots"][chosen.config.slot_index % 4]
        cust.target_x = sx - 0.05
        cust.target_y = sy + 0.05
        cust.state = "walking_machine"
        cust.wait_until = now + 0.35

    async def _on_arrive_machine(self, cust: SimulatedCustomer, now: float) -> None:
        if not cust.target_machine_id:
            self._pick_next_machine(cust, now)
            return

        machine = self.pi_gateway.machines[cust.target_machine_id]

        # Double-tap concurrency demonstration!
        if cust.behavior == "double_tap" and cust.games_played == 0:
            sibling = next(
                (
                    m for m in self.pi_gateway.machines.values()
                    if m.config.site_id == cust.site_id and m.config.id != machine.config.id
                ),
                machine,
            )
            # Force balance low enough or fire 2 simultaneous requests to showcase atomic race protection
            res1, res2 = await asyncio.gather(
                self.pi_gateway.tap_card(machine.config.pi_id, cust.card_uid, bypass_debounce=True),
                self.pi_gateway.tap_card(sibling.config.pi_id, cust.card_uid, bypass_debounce=True),
            )
            cust.games_played += 1
            chosen_res = res1 if res1.approved else res2
            if chosen_res.remaining_balance is not None:
                cust.known_balance = chosen_res.remaining_balance
            cust.bubble_text = "RACE TEST: 2x SIMULTANEOUS TAP"
            cust.bubble_color = "#eab308"
            cust.bubble_until = now + 2.5
            cust.state = "playing"
            cust.wait_until = now + (2.8 / self.settings.speed)
            return

        resp = await self.pi_gateway.tap_card(
            pi_id=machine.config.pi_id,
            card_uid=cust.card_uid,
            bypass_debounce=False,
        )

        if resp.approved:
            cust.games_played += 1
            if resp.remaining_balance is not None:
                cust.known_balance = resp.remaining_balance
            cust.bubble_text = f"-{resp.credits_deducted} CR • PLAY!"
            cust.bubble_color = "#38bdf8"
            cust.bubble_until = now + 2.2
            cust.state = "playing"
            cust.wait_until = now + (3.2 / self.settings.speed)
        elif resp.outcome == TapOutcome.OPERATOR_MISMATCH:
            cust.bubble_text = "BLOCKED: WRONG OPERATOR!"
            cust.bubble_color = "#ef4444"
            cust.bubble_until = now + 2.8
            cust.state = "leaving"
            cust.target_x, cust.target_y = SITE_POSITIONS["entrance"]
            cust.wait_until = now + 1.4
        elif resp.outcome == TapOutcome.INSUFFICIENT_CREDITS:
            cust.known_balance = resp.remaining_balance or 0
            cust.bubble_text = "LOW CREDITS -> KIOSK"
            cust.bubble_color = "#f97316"
            cust.bubble_until = now + 2.0
            if cust.games_played < cust.max_games and cust.site_id in self.sites and self.sites[cust.site_id].operator_id == cust.operator_id:
                cust.target_x, cust.target_y = SITE_POSITIONS["kiosk"]
                cust.state = "walking_kiosk"
            else:
                cust.target_x, cust.target_y = SITE_POSITIONS["entrance"]
                cust.state = "leaving"
            cust.wait_until = now + 0.8
        elif resp.outcome == TapOutcome.CARD_NOT_FOUND:
            # Local site awaiting periodic sync! Wait briefly for next sync, then retry once
            cust.bubble_text = "AWAITING LOCAL SYNC..."
            cust.bubble_color = "#eab308"
            cust.bubble_until = now + 2.5
            cust.wait_until = now + 2.4
            cust.games_played += 1
            if cust.games_played > cust.max_games + 2:
                cust.state = "leaving"
                cust.target_x, cust.target_y = SITE_POSITIONS["entrance"]
        else:
            cust.bubble_text = resp.outcome.value.upper()
            cust.bubble_color = "#ef4444"
            cust.bubble_until = now + 2.0
            cust.wait_until = now + 1.5

    async def _on_finish_play(self, cust: SimulatedCustomer, now: float) -> None:
        # Check if customer should roam to a sibling site (same operator) or rival operator site
        if not cust.roamed_once and cust.games_played >= 1:
            if cust.behavior == "roam_same_op":
                sibling_sites = [
                    s.id for s in self.sites.values()
                    if s.operator_id == cust.operator_id and s.id != cust.site_id
                ]
                if sibling_sites:
                    cust.roamed_once = True
                    cust.site_id = random.choice(sibling_sites)
                    cust.x, cust.y = SITE_POSITIONS["entrance"]
                    cust.bubble_text = "ROAMED (SAME OPERATOR)"
                    cust.bubble_color = "#a855f7"
                    cust.bubble_until = now + 2.6
                    self._pick_next_machine(cust, now)
                    return

            elif cust.behavior == "cross_op_attempt":
                rival_sites = [
                    s.id for s in self.sites.values()
                    if s.operator_id != cust.operator_id
                ]
                if rival_sites:
                    cust.roamed_once = True
                    cust.site_id = random.choice(rival_sites)
                    cust.x, cust.y = SITE_POSITIONS["entrance"]
                    cust.bubble_text = "TRYING RIVAL SITE..."
                    cust.bubble_color = "#f97316"
                    cust.bubble_until = now + 2.4
                    self._pick_next_machine(cust, now)
                    return

        if cust.games_played >= cust.max_games:
            cust.state = "leaving"
            cust.target_x, cust.target_y = SITE_POSITIONS["entrance"]
            cust.bubble_text = "DONE PLAYING"
            cust.bubble_color = "#94a3b8"
            cust.bubble_until = now + 1.5
            return

        self._pick_next_machine(cust, now)

    async def get_snapshot(self) -> dict[str, Any]:
        now = time.time()
        pending_sync = await self.db.get_pending_sync_counts()
        lats = sorted(self.latencies_ms)
        avg_lat = round(sum(lats) / len(lats), 2) if lats else 0.0
        p95_lat = round(lats[min(len(lats) - 1, int(len(lats) * 0.95))], 2) if lats else 0.0

        sites_payload = []
        for s in self.sites.values():
            is_online = not (self.settings.cloud_outage and s.storage_mode == StorageMode.CLOUD)
            last_sync = self.db.last_sync_at.get(s.id, now)
            next_sync_in = (
                max(0.0, round(self.settings.local_sync_interval_sec - (now - last_sync), 1))
                if s.storage_mode == StorageMode.LOCAL
                else 0.0
            )
            sites_payload.append(
                {
                    "id": s.id,
                    "name": s.name,
                    "operator_id": s.operator_id,
                    "storage_mode": s.storage_mode.value,
                    "online": is_online,
                    "wan_online": not self.settings.cloud_outage,
                    "pending_sync_count": pending_sync.get(s.id, 0),
                    "next_sync_in_sec": next_sync_in,
                }
            )

        machines_payload = []
        for m in self.pi_gateway.machines.values():
            pi = self.pi_gateway.pis[m.config.pi_id]
            machines_payload.append(
                {
                    "id": m.config.id,
                    "site_id": m.config.site_id,
                    "name": m.config.name,
                    "game_type": m.config.game_type,
                    "credit_cost": m.config.credit_cost,
                    "pi_id": m.config.pi_id,
                    "slot_index": m.config.slot_index,
                    "status": m.status.value,
                    "active_card_uid": m.active_card_uid,
                    "total_plays": m.total_plays,
                    "total_credits_earned": m.total_credits_earned,
                    "pi_led": pi.last_led,
                    "pi_total_taps": pi.total_taps,
                    "pi_debounced_taps": pi.debounced_taps,
                }
            )

        customers_payload = [
            {
                "id": c.id,
                "name": c.name,
                "card_uid": c.card_uid,
                "operator_id": c.operator_id,
                "site_id": c.site_id,
                "behavior": c.behavior,
                "x": round(c.x, 4),
                "y": round(c.y, 4),
                "state": c.state,
                "known_balance": c.known_balance,
                "color": c.color,
                "bubble_text": c.bubble_text if now < c.bubble_until else None,
                "bubble_color": c.bubble_color,
            }
            for c in self.customers.values()
        ]

        return {
            "timestamp": now,
            "settings": self.settings.model_dump(),
            "operators": [op.model_dump() for op in self.operators.values()],
            "sites": sites_payload,
            "machines": machines_payload,
            "customers": customers_payload,
            "metrics": {
                **self.metrics,
                "avg_latency_ms": avg_lat,
                "p95_latency_ms": p95_lat,
            },
        }

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        if not self.ws_clients:
            return
        dead: list[Any] = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.ws_clients.discard(ws)
