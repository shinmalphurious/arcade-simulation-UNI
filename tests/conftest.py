"""Pytest fixtures providing isolated temp SQLite databases and ASGI HTTP test clients."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from backend.main import create_app


@pytest_asyncio.fixture
async def arcade_client(tmp_path: Path):
    holder: dict[str, object] = {}

    def make_pi_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=holder["app"]),  # type: ignore[arg-type]
            base_url="http://testserver",
        )

    app = create_app(
        data_dir=tmp_path / "db",
        start_simulator=False,
        pi_client_factory=make_pi_client,
    )
    holder["app"] = app

    # Eliminate artificial network sleep for deterministic fast tests unless explicitly configured
    app.state.settings.cloud_latency_ms = 0
    app.state.settings.local_latency_ms = 0
    app.state.settings.running = False

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client, app
