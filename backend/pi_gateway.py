"""Simulated Raspberry Pi NFC Reader firmware and HTTP client gateway."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import httpx

from backend.models import (
    MachineConfig,
    MachineStatus,
    ServerDeductRequest,
    ServerDeductResponse,
    SimulationSettings,
    SiteConfig,
    StorageMode,
    TapOutcome,
)


@dataclass
class MachineRuntimeState:
    config: MachineConfig
    status: MachineStatus = MachineStatus.IDLE
    active_card_uid: Optional[str] = None
    play_ends_at: float = 0.0
    total_plays: int = 0
    total_credits_earned: int = 0
    last_outcome: Optional[TapOutcome] = None
    last_event_at: float = 0.0


@dataclass
class RaspberryPiDevice:
    pi_id: str
    site_id: str
    machine_id: str
    last_tap_ts_by_card: dict[str, float] = field(default_factory=dict)
    total_taps: int = 0
    debounced_taps: int = 0
    last_led: str = "ready"  # ready | green | red | amber


class PiGateway:
    """Routes NFC card taps from Raspberry Pi readers over HTTP to Local or Cloud servers."""

    def __init__(
        self,
        sites: list[SiteConfig],
        machines: list[MachineConfig],
        settings: SimulationSettings,
        client_factory: Optional[Callable[[], httpx.AsyncClient]] = None,
    ) -> None:
        self.sites: dict[str, SiteConfig] = {s.id: s for s in sites}
        self.machines: dict[str, MachineRuntimeState] = {
            m.id: MachineRuntimeState(config=m) for m in machines
        }
        self.pis: dict[str, RaspberryPiDevice] = {
            m.pi_id: RaspberryPiDevice(pi_id=m.pi_id, site_id=m.site_id, machine_id=m.id)
            for m in machines
        }
        self.settings = settings
        self._client_factory = client_factory
        self._client: Optional[httpx.AsyncClient] = None
        self._client_base_url: str = "http://127.0.0.1:8000"
        self.on_event: Optional[Callable[[dict[str, Any]], Any]] = None

    def set_base_url(self, base_url: str) -> None:
        cleaned = base_url.rstrip("/")
        if cleaned != self._client_base_url:
            self._client_base_url = cleaned
            if self._client is not None and self._client_factory is None:
                # Re-create client lazily on next call
                old = self._client
                self._client = None
                try:
                    asyncio.get_running_loop().create_task(old.aclose())
                except RuntimeError:
                    pass

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            if self._client_factory is not None:
                self._client = self._client_factory()
            else:
                self._client = httpx.AsyncClient(
                    base_url=self._client_base_url,
                    timeout=httpx.Timeout(10.0),
                    limits=httpx.Limits(max_connections=600, max_keepalive_connections=300),
                )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def reset_runtime_states(self) -> None:
        for m in self.machines.values():
            m.status = MachineStatus.IDLE
            m.active_card_uid = None
            m.play_ends_at = 0.0
            m.total_plays = 0
            m.total_credits_earned = 0
            m.last_outcome = None
            m.last_event_at = 0.0
        for p in self.pis.values():
            p.last_tap_ts_by_card.clear()
            p.total_taps = 0
            p.debounced_taps = 0
            p.last_led = "ready"

    def tick_machines(self, now: Optional[float] = None) -> None:
        ts = now or time.time()
        for m in self.machines.values():
            if m.status == MachineStatus.PLAYING and ts >= m.play_ends_at:
                m.status = MachineStatus.IDLE
                m.active_card_uid = None
                pi = self.pis[m.config.pi_id]
                pi.last_led = "ready"

    async def tap_card(
        self,
        pi_id: str,
        card_uid: str,
        idempotency_key: Optional[str] = None,
        bypass_debounce: bool = False,
        play_duration_sec: float = 3.5,
    ) -> ServerDeductResponse:
        if pi_id not in self.pis:
            raise KeyError(f"Unknown Raspberry Pi ID: {pi_id}")

        pi = self.pis[pi_id]
        site = self.sites[pi.site_id]
        machine = self.machines[pi.machine_id]
        now = time.time()
        pi.total_taps += 1

        # 1. Hardware NFC Reader Debounce check
        if not bypass_debounce and self.settings.pi_debounce_ms > 0:
            last_ts = pi.last_tap_ts_by_card.get(card_uid, 0.0)
            elapsed_ms = (now - last_ts) * 1000.0
            if elapsed_ms < self.settings.pi_debounce_ms:
                pi.debounced_taps += 1
                pi.last_led = "amber"
                resp = ServerDeductResponse(
                    approved=False,
                    outcome=TapOutcome.DEBOUNCED,
                    message=f"NFC reader debounced duplicate tap ({elapsed_ms:.0f}ms < {self.settings.pi_debounce_ms}ms)",
                    card_uid=card_uid,
                    site_id=site.id,
                    machine_id=machine.config.id,
                    pi_id=pi_id,
                    credits_deducted=0,
                    storage_mode=site.storage_mode,
                    latency_ms=0.0,
                )
                await self._emit_tap_event(resp)
                return resp

        pi.last_tap_ts_by_card[card_uid] = now

        # 2. Build ServerDeductRequest and send real HTTP POST to Local or Cloud endpoint
        req_payload = ServerDeductRequest(
            card_uid=card_uid,
            site_id=site.id,
            operator_id=site.operator_id,
            machine_id=machine.config.id,
            pi_id=pi_id,
            credit_cost=machine.config.credit_cost,
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )

        endpoint = (
            "/api/server/cloud/deduct"
            if site.storage_mode == StorageMode.CLOUD
            else f"/api/server/local/{site.id}/deduct"
        )

        t0 = time.perf_counter()
        client = await self.get_client()
        try:
            http_resp = await client.post(endpoint, json=req_payload.model_dump())
            latency_ms = (time.perf_counter() - t0) * 1000.0
            data = http_resp.json()
            result = ServerDeductResponse(**data)
            result.latency_ms = round(latency_ms, 2)
        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            pi.last_led = "red"
            machine.last_outcome = TapOutcome.SERVER_OFFLINE
            machine.last_event_at = now
            result = ServerDeductResponse(
                approved=False,
                outcome=TapOutcome.SERVER_OFFLINE,
                message=f"Pi HTTP request failed: {exc}",
                card_uid=card_uid,
                site_id=site.id,
                machine_id=machine.config.id,
                pi_id=pi_id,
                credits_deducted=0,
                storage_mode=site.storage_mode,
                latency_ms=round(latency_ms, 2),
            )
            await self._emit_tap_event(result)
            return result

        # 3. Actuate arcade machine credit relay if server approved
        machine.last_outcome = result.outcome
        machine.last_event_at = now
        if result.approved:
            pi.last_led = "green"
            machine.status = MachineStatus.PLAYING
            machine.active_card_uid = card_uid
            machine.play_ends_at = now + (play_duration_sec / max(self.settings.speed, 0.25))
            machine.total_plays += 1
            machine.total_credits_earned += result.credits_deducted
        else:
            pi.last_led = "amber" if result.outcome == TapOutcome.DEBOUNCED else "red"

        await self._emit_tap_event(result)
        return result

    async def _emit_tap_event(self, resp: ServerDeductResponse) -> None:
        if self.on_event is not None:
            payload = {
                "type": "tap_result",
                "timestamp": time.time(),
                **resp.model_dump(mode="json"),
            }
            maybe_coro = self.on_event(payload)
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro
