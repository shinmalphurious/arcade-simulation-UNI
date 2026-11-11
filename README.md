# Arcade NFC Credit Simulation & Stress-Testing Platform

A full-stack interactive simulation and concurrency stress-test suite modeling a multi-operator game arcade where **Raspberry Pi NFC readers** query **Local** or **Cloud** servers over HTTP to authorize and deduct player credits.

## Architecture Overview

- **Multi-Operator & Multi-Site Topology** ([`backend/config.py`](backend/config.py)):
  - **Operator 1 (`NeonPlay Group`)** — 2 sites (`NeonPlay Downtown`, `NeonPlay Westside Mall`) using **Cloud Storage** (real-time credit visibility across all sites owned by the operator).
  - **Operator 2 (`RetroVault Inc.`)** — 2 sites (`RetroVault North`, `RetroVault South`) using **Local Storage** (each site has its own isolated local SQLite DB + periodic idempotent ledger sync between same-operator sites).
  - **Operator Isolation**: Cards issued by one operator are strictly rejected (`403 Forbidden / OPERATOR_MISMATCH`) at any site owned by a different operator.
- **Raspberry Pi NFC Gateway** ([`backend/pi_gateway.py`](backend/pi_gateway.py)):
  - Hardware NFC debounce window to filter accidental rapid re-reads.
  - Sends real HTTP `POST` requests (`httpx.AsyncClient`) to either `/api/server/cloud/deduct` or `/api/server/local/{site_id}/deduct` depending on the site's purchased storage solution.
  - Handles variable credit costs per machine (`1–4` credits) and updates machine relay status.
- **Storage & Distributed Sync Engine** ([`backend/database.py`](backend/database.py)):
  - Separate SQLite databases in `WAL` mode (`cloud.db`, `local_site-retro-north.db`, `local_site-retro-south.db`).
  - Atomic conditional credit deductions (`WHERE balance >= credit_cost`) + server-side `idempotency_cache` prevent double-spending and negative balances under concurrent race conditions.
  - Append-only `ledger_entries` allow periodic cross-site synchronization for Local-tier operators without duplicating or losing credits.
  - **Outage Behavior**: During a simulated Cloud/WAN outage, Cloud-tier sites fail closed (`503`), while Local-tier sites continue operating on their LAN servers and sync once WAN recovers.
- **Visual Simulation UI** ([`frontend/index.html`](frontend/index.html), [`frontend/app.js`](frontend/app.js)):
  - HTML5 Canvas 2D multi-site floorplan with animated customers, Raspberry Pi status LEDs, network HTTP packet pulses, live telemetry, fault-injection controls, manual NFC tap tester, and an embedded stress-test runner.

## Quick Start

### 1. Install Dependencies
```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 2. Start the Visual Simulation Server
```powershell
.venv\Scripts\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```
Then open **http://127.0.0.1:8000** in your browser.

### 3. Run Functional & Concurrency Tests (`pytest`)
```powershell
.venv\Scripts\python.exe -m pytest -v
```

### 4. Run Standalone High-Scale Stress Test (`stress_test.py`)
```powershell
.venv\Scripts\python.exe stress_test.py --requests 1500 --concurrency 80 --cards 60 --credits 30
```
You can also target a live running server:
```powershell
.venv\Scripts\python.exe stress_test.py --url http://127.0.0.1:8000 --requests 1000 --concurrency 60
```
