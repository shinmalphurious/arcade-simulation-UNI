"""FastAPI application hosting Cloud/Local Arcade Servers, Raspberry Pi NFC Gateway, Simulation Engine, and Web UI."""

from __future__ import annotations

import asyncio
import os
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.config import DEFAULT_MACHINES, DEFAULT_OPERATORS, DEFAULT_SITES
from backend.database import ArcadeDatabaseManager
from backend.models import (
    IssueCardRequest,
    PiTapRequest,
    ServerDeductRequest,
    ServerDeductResponse,
    SimulationSettings,
    SimulationSettingsPatch,
    SpawnCustomerRequest,
    StorageMode,
    StressTestRequest,
    TapOutcome,
    TopUpRequest,
)
from backend.pi_gateway import PiGateway
from backend.simulator import ArcadeSimulationEngine
from backend.stress_runner import run_stress_test_against_base_url


FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


def create_app(
    data_dir: Optional[Path] = None,
    start_simulator: bool = True,
    pi_client_factory: Optional[Callable[[], httpx.AsyncClient]] = None,
) -> FastAPI:
    resolved_data_dir = Path(
        data_dir or os.environ.get("ARCADE_DATA_DIR") or (Path(tempfile.gettempdir()) / "arcade_sim_data")
    )
    settings = SimulationSettings()
    db = ArcadeDatabaseManager(data_dir=resolved_data_dir, sites=DEFAULT_SITES)
    pi_gateway = PiGateway(
        sites=DEFAULT_SITES,
        machines=DEFAULT_MACHINES,
        settings=settings,
        client_factory=pi_client_factory,
    )
    sim = ArcadeSimulationEngine(
        db=db,
        pi_gateway=pi_gateway,
        settings=settings,
        operators=DEFAULT_OPERATORS,
        sites=DEFAULT_SITES,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await db.initialize()
        if os.environ.get("ARCADE_BASE_URL"):
            pi_gateway.set_base_url(os.environ["ARCADE_BASE_URL"])
        if start_simulator:
            sim.start()
        try:
            yield
        finally:
            await sim.stop()
            await pi_gateway.close()
            await db.close()

    app = FastAPI(
        title="Arcade NFC Credit Simulation & Stress Test Platform",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Expose internals on app.state for tests and diagnostics
    app.state.db = db
    app.state.pi_gateway = pi_gateway
    app.state.sim = sim
    app.state.settings = settings

    if FRONTEND_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_index(request: Request):
        if pi_client_factory is None:
            pi_gateway.set_base_url(str(request.base_url))
        index_path = FRONTEND_DIR / "index.html"
        if not index_path.exists():
            return JSONResponse({"status": "ok", "message": "Arcade Simulation API running"})
        return FileResponse(index_path)

    # ------------------------------------------------------------------
    # Kiosk / POS Endpoints (Customer Card Issuance & Credit Top-Ups)
    # ------------------------------------------------------------------
    @app.post("/api/kiosk/issue-card")
    async def kiosk_issue_card(req: IssueCardRequest):
        if req.site_id not in db.sites:
            raise HTTPException(status_code=404, detail=f"Unknown site_id: {req.site_id}")
        site = db.sites[req.site_id]
        if site.storage_mode == StorageMode.CLOUD and settings.cloud_outage:
            raise HTTPException(
                status_code=503,
                detail="Cloud server unreachable during outage; cloud kiosk cannot issue/top-up cards",
            )
        card_uid = req.card_uid or ":".join(f"{b:02X}" for b in uuid.uuid4().bytes[:5])
        try:
            result = await db.issue_or_topup_card(
                site_id=req.site_id,
                card_uid=card_uid,
                customer_name=req.customer_name,
                credits=req.initial_credits,
                tx_type="issue",
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        sim.metrics["topups"] += 1
        return result

    @app.post("/api/kiosk/topup")
    async def kiosk_topup(req: TopUpRequest):
        if req.site_id not in db.sites:
            raise HTTPException(status_code=404, detail=f"Unknown site_id: {req.site_id}")
        site = db.sites[req.site_id]
        if site.storage_mode == StorageMode.CLOUD and settings.cloud_outage:
            raise HTTPException(
                status_code=503,
                detail="Cloud server unreachable during outage; cloud kiosk cannot process top-ups",
            )
        try:
            result = await db.issue_or_topup_card(
                site_id=req.site_id,
                card_uid=req.card_uid,
                customer_name="Existing Player",
                credits=req.credits,
                tx_type="topup",
                idempotency_key=req.idempotency_key,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        sim.metrics["topups"] += 1
        return result

    # ------------------------------------------------------------------
    # Raspberry Pi NFC Reader Gateway Endpoint
    # ------------------------------------------------------------------
    @app.post("/api/pi/{pi_id}/tap", response_model=ServerDeductResponse)
    async def pi_nfc_tap(pi_id: str, req: PiTapRequest, request: Request):
        if pi_client_factory is None:
            pi_gateway.set_base_url(str(request.base_url))
        if pi_id not in pi_gateway.pis:
            raise HTTPException(status_code=404, detail=f"Unknown Raspberry Pi ID: {pi_id}")
        resp = await pi_gateway.tap_card(
            pi_id=pi_id,
            card_uid=req.card_uid,
            idempotency_key=req.idempotency_key,
            bypass_debounce=req.bypass_debounce,
        )
        status_map = {
            TapOutcome.APPROVED: 200,
            TapOutcome.DUPLICATE_IDEMPOTENCY: 200,
            TapOutcome.DEBOUNCED: 429,
            TapOutcome.INSUFFICIENT_CREDITS: 402,
            TapOutcome.OPERATOR_MISMATCH: 403,
            TapOutcome.CARD_NOT_FOUND: 404,
            TapOutcome.SERVER_OFFLINE: 503,
        }
        return JSONResponse(status_code=status_map.get(resp.outcome, 400), content=resp.model_dump(mode="json"))

    # ------------------------------------------------------------------
    # Authoritative Cloud & Local Site Server Endpoints (called by Pis)
    # ------------------------------------------------------------------
    async def _handle_deduct(
        req: ServerDeductRequest, expected_mode: StorageMode, target_site_id: Optional[str] = None
    ) -> JSONResponse:
        if req.site_id not in db.sites:
            raise HTTPException(status_code=404, detail=f"Unknown site_id: {req.site_id}")
        site = db.sites[req.site_id]
        if site.storage_mode != expected_mode:
            raise HTTPException(
                status_code=400,
                detail=f"Site {req.site_id} is configured for {site.storage_mode.value}, not {expected_mode.value}",
            )
        if target_site_id is not None and req.site_id != target_site_id:
            raise HTTPException(
                status_code=400,
                detail=f"Local server {target_site_id} cannot process request for {req.site_id}",
            )

        # Simulate realistic network latency
        latency_ms = (
            settings.cloud_latency_ms
            if expected_mode == StorageMode.CLOUD
            else settings.local_latency_ms
        )
        if latency_ms > 0:
            await asyncio.sleep(latency_ms / 1000.0)

        # Cloud outage blocks Cloud-tier requests, whereas Local-tier LAN servers remain operational!
        if expected_mode == StorageMode.CLOUD and settings.cloud_outage:
            payload = ServerDeductResponse(
                approved=False,
                outcome=TapOutcome.SERVER_OFFLINE,
                message="Cloud server unreachable (simulated WAN/Cloud outage)",
                card_uid=req.card_uid,
                operator_id=req.operator_id,
                site_id=req.site_id,
                machine_id=req.machine_id,
                pi_id=req.pi_id,
                credits_deducted=0,
                remaining_balance=None,
                storage_mode=expected_mode,
            )
            return JSONResponse(status_code=503, content=payload.model_dump(mode="json"))

        res = await db.deduct_credit(
            site_id=req.site_id,
            operator_id=site.operator_id,
            machine_id=req.machine_id,
            card_uid=req.card_uid,
            credit_cost=req.credit_cost,
            idempotency_key=req.idempotency_key,
        )

        payload = ServerDeductResponse(
            approved=res["approved"],
            outcome=res["outcome"],
            message=res["message"],
            card_uid=req.card_uid,
            operator_id=res["operator_id"],
            site_id=req.site_id,
            machine_id=req.machine_id,
            pi_id=req.pi_id,
            credits_deducted=res["credits_deducted"],
            remaining_balance=res["remaining_balance"],
            storage_mode=expected_mode,
            tx_id=res["tx_id"],
        )

        status_map = {
            TapOutcome.APPROVED: 200,
            TapOutcome.DUPLICATE_IDEMPOTENCY: 200,
            TapOutcome.INSUFFICIENT_CREDITS: 402,
            TapOutcome.OPERATOR_MISMATCH: 403,
            TapOutcome.CARD_NOT_FOUND: 404,
        }
        return JSONResponse(
            status_code=status_map.get(res["outcome"], 400),
            content=payload.model_dump(mode="json"),
        )

    @app.post("/api/server/cloud/deduct")
    async def cloud_server_deduct(req: ServerDeductRequest):
        return await _handle_deduct(req, expected_mode=StorageMode.CLOUD)

    @app.post("/api/server/local/{site_id}/deduct")
    async def local_server_deduct(site_id: str, req: ServerDeductRequest):
        return await _handle_deduct(req, expected_mode=StorageMode.LOCAL, target_site_id=site_id)

    # ------------------------------------------------------------------
    # Simulation Control, Inspection & Stress-Test Endpoints
    # ------------------------------------------------------------------
    @app.get("/api/sim/snapshot")
    async def get_snapshot():
        return await sim.get_snapshot()

    @app.get("/api/sim/accounts")
    async def get_accounts():
        return {"accounts": await db.get_all_accounts()}

    @app.get("/api/sim/events")
    async def get_events():
        return {"events": list(sim.event_log)}

    @app.patch("/api/sim/settings")
    async def patch_settings(patch: SimulationSettingsPatch):
        data = patch.model_dump(exclude_none=True)
        for k, v in data.items():
            setattr(settings, k, v)
        return {"settings": settings.model_dump()}

    @app.post("/api/sim/spawn")
    async def spawn_customer(req: SpawnCustomerRequest):
        cust = sim.spawn_customer(
            site_id=req.site_id,
            customer_name=req.customer_name,
            initial_credits=req.initial_credits,
            behavior=req.behavior,
        )
        return {"customer_id": cust.id, "card_uid": cust.card_uid, "site_id": cust.site_id}

    @app.post("/api/sim/sync-local")
    async def manual_local_sync():
        results = await sim.trigger_local_sync(manual=True)
        return {"synced": results}

    @app.post("/api/sim/reset")
    async def reset_simulation():
        await sim.reset()
        return {"status": "reset"}

    @app.post("/api/sim/double-tap-demo")
    async def double_tap_demo(site_id: str, card_uid: str):
        site_machines = [m for m in DEFAULT_MACHINES if m.site_id == site_id]
        if len(site_machines) < 2:
            raise HTTPException(status_code=400, detail="Need at least 2 machines at site")
        m1, m2 = site_machines[0], site_machines[1]
        r1, r2 = await asyncio.gather(
            pi_gateway.tap_card(m1.pi_id, card_uid, bypass_debounce=True),
            pi_gateway.tap_card(m2.pi_id, card_uid, bypass_debounce=True),
        )
        return {"tap_1": r1.model_dump(), "tap_2": r2.model_dump()}

    @app.get("/api/audit")
    async def audit_system():
        return await db.audit_invariants()

    @app.post("/api/stress-test/run")
    async def run_stress_test(req: StressTestRequest, request: Request):
        # Pause autonomous background traffic during stress test so invariant accounting is crystal clear
        was_running = settings.running
        orig_cloud_lat = settings.cloud_latency_ms
        orig_local_lat = settings.local_latency_ms
        settings.running = False
        # Zero artificial sleep during raw throughput stress test unless user wants latency tested separately
        settings.cloud_latency_ms = 0
        settings.local_latency_ms = 0
        try:
            await sim.reset()
            transport = None
            if pi_client_factory is not None:
                # If running under ASGI test client
                transport = httpx.ASGITransport(app=app)
            result = await run_stress_test_against_base_url(
                base_url=str(request.base_url),
                req=req,
                transport=transport,
            )
            return result
        finally:
            settings.cloud_latency_ms = orig_cloud_lat
            settings.local_latency_ms = orig_local_lat
            settings.running = was_running

    @app.websocket("/ws/events")
    async def websocket_events(websocket: WebSocket):
        await websocket.accept()
        sim.ws_clients.add(websocket)
        try:
            await websocket.send_json({"type": "snapshot", "snapshot": await sim.get_snapshot()})
            while True:
                # Keep connection alive; handle optional ping messages
                await websocket.receive_text()
        except WebSocketDisconnect:
            sim.ws_clients.discard(websocket)
        except Exception:
            sim.ws_clients.discard(websocket)

    return app


app = create_app()
