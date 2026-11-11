"""Async SQLite storage engine modeling separate Cloud DB and per-site Local DBs with atomic deductions and idempotent periodic ledger sync."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from backend.models import SiteConfig, StorageMode, TapOutcome


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS accounts (
    card_uid TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0 CHECK (balance >= 0),
    version INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    tx_id TEXT PRIMARY KEY,
    card_uid TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    delta INTEGER NOT NULL,
    tx_type TEXT NOT NULL,
    origin_site_id TEXT NOT NULL,
    machine_id TEXT,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_card ON ledger_entries(card_uid);
CREATE INDEX IF NOT EXISTS idx_ledger_origin ON ledger_entries(origin_site_id);

CREATE TABLE IF NOT EXISTS idempotency_cache (
    idempotency_key TEXT PRIMARY KEY,
    tx_id TEXT,
    outcome TEXT NOT NULL,
    message TEXT NOT NULL,
    credits_deducted INTEGER NOT NULL,
    remaining_balance INTEGER,
    operator_id TEXT,
    created_at REAL NOT NULL
);
"""


@dataclass
class DBHandle:
    name: str
    path: Path
    conn: Optional[aiosqlite.Connection] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class ArcadeDatabaseManager:
    """Manages one shared Cloud SQLite DB and one isolated Local SQLite DB per local-tier site."""

    def __init__(self, data_dir: Path, sites: list[SiteConfig]) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sites: dict[str, SiteConfig] = {s.id: s for s in sites}
        self.cloud_db = DBHandle(name="cloud", path=self.data_dir / "cloud.db")
        self.local_dbs: dict[str, DBHandle] = {
            s.id: DBHandle(name=f"local:{s.id}", path=self.data_dir / f"local_{s.id}.db")
            for s in sites
            if s.storage_mode == StorageMode.LOCAL
        }
        self.last_sync_at: dict[str, float] = {
            s.id: time.time() for s in sites if s.storage_mode == StorageMode.LOCAL
        }
        self.total_sync_events: int = 0

    async def initialize(self) -> None:
        await self._open_handle(self.cloud_db)
        for handle in self.local_dbs.values():
            await self._open_handle(handle)

    async def close(self) -> None:
        if self.cloud_db.conn is not None:
            await self.cloud_db.conn.close()
            self.cloud_db.conn = None
        for handle in self.local_dbs.values():
            if handle.conn is not None:
                await handle.conn.close()
                handle.conn = None

    async def reset(self) -> None:
        await self.close()
        for db_path in [self.cloud_db.path, *(h.path for h in self.local_dbs.values())]:
            for suffix in ("", "-wal", "-shm"):
                p = Path(f"{db_path}{suffix}")
                if p.exists():
                    try:
                        os.remove(p)
                    except OSError:
                        pass
        now = time.time()
        for sid in self.last_sync_at:
            self.last_sync_at[sid] = now
        self.total_sync_events = 0
        await self.initialize()

    async def _open_handle(self, handle: DBHandle) -> None:
        handle.conn = await aiosqlite.connect(handle.path)
        handle.conn.row_factory = aiosqlite.Row
        await handle.conn.executescript(SCHEMA_SQL)
        await handle.conn.commit()

    def get_handle_for_site(self, site_id: str) -> DBHandle:
        site = self.sites[site_id]
        if site.storage_mode == StorageMode.CLOUD:
            return self.cloud_db
        return self.local_dbs[site_id]

    async def issue_or_topup_card(
        self,
        site_id: str,
        card_uid: str,
        customer_name: str,
        credits: int,
        tx_type: str = "topup",
        idempotency_key: Optional[str] = None,
    ) -> dict[str, Any]:
        site = self.sites[site_id]
        handle = self.get_handle_for_site(site_id)
        now = time.time()
        tx_id = str(uuid.uuid4())
        idem_key = idempotency_key or f"topup:{tx_id}"

        # Enforce cross-operator isolation before opening transaction (no nested lock held)
        other_op = await self._lookup_card_operator_elsewhere(card_uid, exclude_handle=handle)
        if other_op is not None and other_op != site.operator_id:
            raise PermissionError(
                f"Card {card_uid} belongs to operator {other_op}, cannot top up at {site.operator_id}"
            )

        async with handle.lock:
            conn = handle.conn
            assert conn is not None
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Idempotency check for topups
                cur = await conn.execute(
                    "SELECT * FROM idempotency_cache WHERE idempotency_key = ?",
                    (idem_key,),
                )
                cached = await cur.fetchone()
                if cached is not None:
                    await conn.rollback()
                    return {
                        "card_uid": card_uid,
                        "operator_id": cached["operator_id"],
                        "balance": cached["remaining_balance"],
                        "tx_id": cached["tx_id"],
                        "storage_mode": site.storage_mode.value,
                        "duplicate": True,
                    }

                cur = await conn.execute(
                    "SELECT * FROM accounts WHERE card_uid = ?",
                    (card_uid,),
                )
                existing = await cur.fetchone()
                if existing is not None:
                    if existing["operator_id"] != site.operator_id:
                        await conn.rollback()
                        raise PermissionError(
                            f"Card {card_uid} belongs to operator {existing['operator_id']}, cannot top up at {site.operator_id}"
                        )
                    new_balance = int(existing["balance"]) + credits
                    await conn.execute(
                        "UPDATE accounts SET balance = ?, version = version + 1, updated_at = ? WHERE card_uid = ?",
                        (new_balance, now, card_uid),
                    )
                    resolved_name = existing["customer_name"]
                else:
                    new_balance = credits
                    resolved_name = customer_name
                    await conn.execute(
                        """
                        INSERT INTO accounts (card_uid, customer_name, operator_id, balance, version, created_at, updated_at)
                        VALUES (?, ?, ?, ?, 1, ?, ?)
                        """,
                        (card_uid, resolved_name, site.operator_id, new_balance, now, now),
                    )

                if credits > 0:
                    await conn.execute(
                        """
                        INSERT INTO ledger_entries (tx_id, card_uid, customer_name, operator_id, delta, tx_type, origin_site_id, machine_id, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
                        """,
                        (tx_id, card_uid, resolved_name, site.operator_id, credits, tx_type, site_id, now),
                    )

                await conn.execute(
                    """
                    INSERT INTO idempotency_cache (idempotency_key, tx_id, outcome, message, credits_deducted, remaining_balance, operator_id, created_at)
                    VALUES (?, ?, 'approved', 'Top-up applied', ?, ?, ?, ?)
                    """,
                    (idem_key, tx_id, -credits, new_balance, site.operator_id, now),
                )
                await conn.commit()
            except Exception:
                if conn.in_transaction:
                    await conn.rollback()
                raise

        return {
            "card_uid": card_uid,
            "customer_name": resolved_name,
            "operator_id": site.operator_id,
            "balance": new_balance,
            "tx_id": tx_id,
            "storage_mode": site.storage_mode.value,
            "duplicate": False,
        }

    async def deduct_credit(
        self,
        site_id: str,
        operator_id: str,
        machine_id: str,
        card_uid: str,
        credit_cost: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Atomically checks card ownership, operator isolation, idempotency, and credit balance."""
        site = self.sites[site_id]
        handle = self.get_handle_for_site(site_id)
        now = time.time()
        tx_id = str(uuid.uuid4())
        missing_locally = False

        async with handle.lock:
            conn = handle.conn
            assert conn is not None
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # 1. Server-side idempotency deduplication (handles Pi network retries safely)
                cur = await conn.execute(
                    "SELECT * FROM idempotency_cache WHERE idempotency_key = ?",
                    (idempotency_key,),
                )
                cached = await cur.fetchone()
                if cached is not None:
                    await conn.commit()
                    return {
                        "approved": cached["outcome"] == TapOutcome.APPROVED.value,
                        "outcome": TapOutcome.DUPLICATE_IDEMPOTENCY,
                        "message": f"Idempotent replay ({cached['outcome']}): {cached['message']}",
                        "operator_id": cached["operator_id"],
                        "credits_deducted": 0,
                        "remaining_balance": cached["remaining_balance"],
                        "tx_id": cached["tx_id"],
                    }

                # 2. Fetch account in this DB (Cloud DB or Local Site DB)
                cur = await conn.execute(
                    "SELECT * FROM accounts WHERE card_uid = ?",
                    (card_uid,),
                )
                acc = await cur.fetchone()

                if acc is None:
                    await conn.commit()
                    missing_locally = True
                else:
                    # 3. Enforce Operator Isolation
                    if acc["operator_id"] != operator_id:
                        await conn.execute(
                            """
                            INSERT INTO idempotency_cache (idempotency_key, tx_id, outcome, message, credits_deducted, remaining_balance, operator_id, created_at)
                            VALUES (?, NULL, ?, ?, 0, NULL, ?, ?)
                            """,
                            (
                                idempotency_key,
                                TapOutcome.OPERATOR_MISMATCH.value,
                                f"Card belongs to {acc['operator_id']}; rejected at {operator_id}",
                                acc["operator_id"],
                                now,
                            ),
                        )
                        await conn.commit()
                        return {
                            "approved": False,
                            "outcome": TapOutcome.OPERATOR_MISMATCH,
                            "message": f"Card belongs to {acc['operator_id']}; rejected at {operator_id}",
                            "operator_id": acc["operator_id"],
                            "credits_deducted": 0,
                            "remaining_balance": None,
                            "tx_id": None,
                        }

                    # 4. Atomic conditional deduction preventing double-spend / negative balance
                    update_cur = await conn.execute(
                        """
                        UPDATE accounts
                        SET balance = balance - ?, version = version + 1, updated_at = ?
                        WHERE card_uid = ? AND operator_id = ? AND balance >= ?
                        """,
                        (credit_cost, now, card_uid, operator_id, credit_cost),
                    )
                    if update_cur.rowcount != 1:
                        cur = await conn.execute(
                            "SELECT balance FROM accounts WHERE card_uid = ?",
                            (card_uid,),
                        )
                        bal_row = await cur.fetchone()
                        current_bal = int(bal_row["balance"]) if bal_row else 0
                        msg = f"Insufficient credits: needs {credit_cost}, has {current_bal}"
                        await conn.execute(
                            """
                            INSERT INTO idempotency_cache (idempotency_key, tx_id, outcome, message, credits_deducted, remaining_balance, operator_id, created_at)
                            VALUES (?, NULL, ?, ?, 0, ?, ?, ?)
                            """,
                            (
                                idempotency_key,
                                TapOutcome.INSUFFICIENT_CREDITS.value,
                                msg,
                                current_bal,
                                operator_id,
                                now,
                            ),
                        )
                        await conn.commit()
                        return {
                            "approved": False,
                            "outcome": TapOutcome.INSUFFICIENT_CREDITS,
                            "message": msg,
                            "operator_id": operator_id,
                            "credits_deducted": 0,
                            "remaining_balance": current_bal,
                            "tx_id": None,
                        }

                    # Fetch updated balance
                    cur = await conn.execute(
                        "SELECT balance FROM accounts WHERE card_uid = ?",
                        (card_uid,),
                    )
                    new_bal = int((await cur.fetchone())["balance"])

                    await conn.execute(
                        """
                        INSERT INTO ledger_entries (tx_id, card_uid, customer_name, operator_id, delta, tx_type, origin_site_id, machine_id, created_at)
                        VALUES (?, ?, ?, ?, ?, 'deduct', ?, ?, ?)
                        """,
                        (
                            tx_id,
                            card_uid,
                            acc["customer_name"],
                            operator_id,
                            -credit_cost,
                            site_id,
                            machine_id,
                            now,
                        ),
                    )
                    await conn.execute(
                        """
                        INSERT INTO idempotency_cache (idempotency_key, tx_id, outcome, message, credits_deducted, remaining_balance, operator_id, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            idempotency_key,
                            tx_id,
                            TapOutcome.APPROVED.value,
                            f"Applied {credit_cost} credit(s) to {machine_id}",
                            credit_cost,
                            new_bal,
                            operator_id,
                            now,
                        ),
                    )
                    await conn.commit()
                    return {
                        "approved": True,
                        "outcome": TapOutcome.APPROVED,
                        "message": f"Applied {credit_cost} credit(s) to {machine_id}",
                        "operator_id": operator_id,
                        "credits_deducted": credit_cost,
                        "remaining_balance": new_bal,
                        "tx_id": tx_id,
                    }
            except Exception:
                if conn.in_transaction:
                    await conn.rollback()
                raise

        if missing_locally:
            # Perform cross-DB lookup strictly outside handle.lock to avoid AB-BA lock-order deadlocks
            other_op = await self._lookup_card_operator_elsewhere(card_uid, exclude_handle=handle)
            if other_op is not None and other_op != operator_id:
                return {
                    "approved": False,
                    "outcome": TapOutcome.OPERATOR_MISMATCH,
                    "message": f"Card belongs to {other_op}; rejected at {operator_id}",
                    "operator_id": other_op,
                    "credits_deducted": 0,
                    "remaining_balance": None,
                    "tx_id": None,
                }
            if other_op == operator_id and site.storage_mode == StorageMode.LOCAL:
                return {
                    "approved": False,
                    "outcome": TapOutcome.CARD_NOT_FOUND,
                    "message": "Card created at sibling local site; awaiting periodic sync",
                    "operator_id": other_op,
                    "credits_deducted": 0,
                    "remaining_balance": None,
                    "tx_id": None,
                }
            return {
                "approved": False,
                "outcome": TapOutcome.CARD_NOT_FOUND,
                "message": "Card ID not registered on this server",
                "operator_id": None,
                "credits_deducted": 0,
                "remaining_balance": None,
                "tx_id": None,
            }

    async def _lookup_card_operator_elsewhere(
        self, card_uid: str, exclude_handle: DBHandle
    ) -> Optional[str]:
        handles = [self.cloud_db, *self.local_dbs.values()]
        for h in handles:
            if h is exclude_handle or h.conn is None:
                continue
            async with h.lock:
                cur = await h.conn.execute(
                    "SELECT operator_id FROM accounts WHERE card_uid = ?",
                    (card_uid,),
                )
                row = await cur.fetchone()
                if row is not None:
                    return str(row["operator_id"])
        return None

    async def sync_local_sites_for_operator(self, operator_id: str) -> dict[str, Any]:
        """Periodically synchronizes ledger entries across all local-tier sites owned by the same operator."""
        target_sites = [
            s
            for s in self.sites.values()
            if s.operator_id == operator_id and s.storage_mode == StorageMode.LOCAL
        ]
        if len(target_sites) < 2:
            return {"operator_id": operator_id, "sites_synced": len(target_sites), "entries_replicated": 0}

        # 1. Gather all ledger entries across same-operator local DBs
        all_entries: dict[str, dict[str, Any]] = {}
        for s in target_sites:
            handle = self.local_dbs[s.id]
            async with handle.lock:
                assert handle.conn is not None
                cur = await handle.conn.execute("SELECT * FROM ledger_entries")
                rows = await cur.fetchall()
                for r in rows:
                    all_entries[r["tx_id"]] = dict(r)

        if not all_entries:
            now = time.time()
            for s in target_sites:
                self.last_sync_at[s.id] = now
            return {"operator_id": operator_id, "sites_synced": len(target_sites), "entries_replicated": 0}

        # 2. Replicate missing entries into each site DB and recompute materialized balances idempotently
        replicated_count = 0
        now = time.time()
        for s in target_sites:
            handle = self.local_dbs[s.id]
            async with handle.lock:
                assert handle.conn is not None
                await handle.conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = await handle.conn.execute("SELECT tx_id FROM ledger_entries")
                    existing_ids = {r["tx_id"] for r in await cur.fetchall()}

                    missing = [e for tid, e in all_entries.items() if tid not in existing_ids]
                    affected_cards: set[str] = set()
                    for e in missing:
                        await handle.conn.execute(
                            """
                            INSERT OR IGNORE INTO ledger_entries
                            (tx_id, card_uid, customer_name, operator_id, delta, tx_type, origin_site_id, machine_id, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                e["tx_id"],
                                e["card_uid"],
                                e["customer_name"],
                                e["operator_id"],
                                e["delta"],
                                e["tx_type"],
                                e["origin_site_id"],
                                e["machine_id"],
                                e["created_at"],
                            ),
                        )
                        affected_cards.add(e["card_uid"])
                        replicated_count += 1

                    # Recompute exact materialized balance from full ledger for affected cards
                    for card_uid in affected_cards:
                        cur = await handle.conn.execute(
                            """
                            SELECT customer_name, operator_id, COALESCE(SUM(delta), 0) AS bal, MIN(created_at) AS first_ts
                            FROM ledger_entries
                            WHERE card_uid = ?
                            """,
                            (card_uid,),
                        )
                        agg = await cur.fetchone()
                        if agg is None or agg["operator_id"] is None:
                            continue
                        recomputed_bal = max(0, int(agg["bal"]))
                        await handle.conn.execute(
                            """
                            INSERT INTO accounts (card_uid, customer_name, operator_id, balance, version, created_at, updated_at)
                            VALUES (?, ?, ?, ?, 1, ?, ?)
                            ON CONFLICT(card_uid) DO UPDATE SET
                                balance = excluded.balance,
                                version = accounts.version + 1,
                                updated_at = excluded.updated_at
                            """,
                            (
                                card_uid,
                                agg["customer_name"],
                                agg["operator_id"],
                                recomputed_bal,
                                float(agg["first_ts"] or now),
                                now,
                            ),
                        )

                    await handle.conn.commit()
                except Exception:
                    if handle.conn.in_transaction:
                        await handle.conn.rollback()
                    raise
            self.last_sync_at[s.id] = now

        if replicated_count > 0:
            self.total_sync_events += 1

        return {
            "operator_id": operator_id,
            "sites_synced": len(target_sites),
            "entries_replicated": replicated_count,
            "synced_at": now,
        }

    async def get_pending_sync_counts(self) -> dict[str, int]:
        """Returns how many ledger entries each local site has not yet received from sibling local sites."""
        by_op: dict[str, list[str]] = {}
        for s in self.sites.values():
            if s.storage_mode == StorageMode.LOCAL:
                by_op.setdefault(s.operator_id, []).append(s.id)

        pending: dict[str, int] = {}
        for op_id, site_ids in by_op.items():
            site_txs: dict[str, set[str]] = {}
            union_txs: set[str] = set()
            for sid in site_ids:
                h = self.local_dbs[sid]
                async with h.lock:
                    assert h.conn is not None
                    cur = await h.conn.execute("SELECT tx_id FROM ledger_entries")
                    txs = {r["tx_id"] for r in await cur.fetchall()}
                    site_txs[sid] = txs
                    union_txs |= txs
            for sid in site_ids:
                pending[sid] = len(union_txs - site_txs[sid])
        return pending

    async def get_all_accounts(self) -> list[dict[str, Any]]:
        """Returns consolidated view of accounts across Cloud DB and Local Site DBs."""
        results: list[dict[str, Any]] = []
        async with self.cloud_db.lock:
            assert self.cloud_db.conn is not None
            cur = await self.cloud_db.conn.execute("SELECT * FROM accounts ORDER BY created_at DESC")
            for r in await cur.fetchall():
                d = dict(r)
                d["storage_mode"] = "cloud"
                d["site_db"] = "cloud"
                results.append(d)

        # Merge local site views by card_uid (show latest updated + per-site breakdown)
        local_cards: dict[str, dict[str, Any]] = {}
        for sid, h in self.local_dbs.items():
            async with h.lock:
                assert h.conn is not None
                cur = await h.conn.execute("SELECT * FROM accounts ORDER BY created_at DESC")
                for r in await cur.fetchall():
                    d = dict(r)
                    uid = d["card_uid"]
                    if uid not in local_cards:
                        local_cards[uid] = {
                            **d,
                            "storage_mode": "local",
                            "site_db": sid,
                            "site_balances": {sid: d["balance"]},
                        }
                    else:
                        local_cards[uid]["site_balances"][sid] = d["balance"]
                        if d["updated_at"] >= local_cards[uid]["updated_at"]:
                            local_cards[uid]["balance"] = d["balance"]
                            local_cards[uid]["updated_at"] = d["updated_at"]
                            local_cards[uid]["site_db"] = sid
        results.extend(local_cards.values())
        return results

    async def get_card_account(self, site_id: str, card_uid: str) -> Optional[dict[str, Any]]:
        handle = self.get_handle_for_site(site_id)
        async with handle.lock:
            assert handle.conn is not None
            cur = await handle.conn.execute("SELECT * FROM accounts WHERE card_uid = ?", (card_uid,))
            row = await cur.fetchone()
            return dict(row) if row is not None else None

    async def audit_invariants(self) -> dict[str, Any]:
        """Audits conservation of credits, zero negative balances, and zero cross-operator leaks."""
        # First sync local sites so distributed ledgers can be audited cleanly (also check pre-sync negatives!)
        negative_accounts: list[dict[str, Any]] = []
        cross_op_leaks: list[dict[str, Any]] = []

        # Check Cloud DB
        async with self.cloud_db.lock:
            assert self.cloud_db.conn is not None
            cur = await self.cloud_db.conn.execute("SELECT * FROM accounts WHERE balance < 0")
            negative_accounts.extend(dict(r) for r in await cur.fetchall())

            cur = await self.cloud_db.conn.execute(
                "SELECT COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0) AS topped_up, "
                "COALESCE(SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END), 0) AS deducted FROM ledger_entries"
            )
            cloud_ledger = dict(await cur.fetchone())

            cur = await self.cloud_db.conn.execute("SELECT COALESCE(SUM(balance), 0) AS bal FROM accounts")
            cloud_bal = int((await cur.fetchone())["bal"])

            cur = await self.cloud_db.conn.execute("SELECT operator_id, origin_site_id, tx_id FROM ledger_entries")
            for row in await cur.fetchall():
                site = self.sites.get(row["origin_site_id"])
                if site and site.operator_id != row["operator_id"]:
                    cross_op_leaks.append(dict(row))

        # Check Local DBs (deduplicate ledger entries across same-operator sites by tx_id)
        local_txs: dict[str, dict[str, Any]] = {}
        for sid, h in self.local_dbs.items():
            async with h.lock:
                assert h.conn is not None
                cur = await h.conn.execute("SELECT * FROM accounts WHERE balance < 0")
                negative_accounts.extend(dict(r) for r in await cur.fetchall())

                cur = await h.conn.execute("SELECT * FROM ledger_entries")
                for row in await cur.fetchall():
                    d = dict(row)
                    local_txs[d["tx_id"]] = d
                    site = self.sites.get(d["origin_site_id"])
                    if site and site.operator_id != d["operator_id"]:
                        cross_op_leaks.append(d)

        local_topped_up = sum(t["delta"] for t in local_txs.values() if t["delta"] > 0)
        local_deducted = sum(-t["delta"] for t in local_txs.values() if t["delta"] < 0)

        # Compute global local balance per card from unique ledger entries (and verify zero negative net balances)
        local_net_by_card: dict[str, int] = {}
        for t in local_txs.values():
            local_net_by_card[t["card_uid"]] = local_net_by_card.get(t["card_uid"], 0) + t["delta"]
        local_overdrafts = [uid for uid, bal in local_net_by_card.items() if bal < 0]
        local_bal_expected = sum(local_net_by_card.values())

        cloud_conserved = (cloud_ledger["topped_up"] - cloud_ledger["deducted"]) == cloud_bal
        local_conserved = (
            (local_topped_up - local_deducted) == local_bal_expected
            and len(local_overdrafts) == 0
        )

        return {
            "passed": (
                len(negative_accounts) == 0
                and len(cross_op_leaks) == 0
                and cloud_conserved
                and local_conserved
            ),
            "negative_balance_count": len(negative_accounts) + len(local_overdrafts),
            "cross_operator_leak_count": len(cross_op_leaks),
            "cloud": {
                "topped_up": cloud_ledger["topped_up"],
                "deducted": cloud_ledger["deducted"],
                "remaining_balance": cloud_bal,
                "conserved": cloud_conserved,
            },
            "local": {
                "topped_up": local_topped_up,
                "deducted": local_deducted,
                "remaining_balance": local_bal_expected,
                "conserved": local_conserved,
                "overdraft_cards": len(local_overdrafts),
                "unique_ledger_txs": len(local_txs),
            },
        }
