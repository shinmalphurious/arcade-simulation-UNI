"""Domain models and Pydantic request/response schemas for the Arcade NFC Simulation."""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional
from pydantic import BaseModel, Field


class StorageMode(str, Enum):
    CLOUD = "cloud"
    LOCAL = "local"


class MachineStatus(str, Enum):
    IDLE = "idle"
    PLAYING = "playing"
    ERROR = "error"


class TapOutcome(str, Enum):
    APPROVED = "approved"
    INSUFFICIENT_CREDITS = "insufficient_credits"
    OPERATOR_MISMATCH = "operator_mismatch"
    CARD_NOT_FOUND = "card_not_found"
    DEBOUNCED = "debounced"
    DUPLICATE_IDEMPOTENCY = "duplicate_idempotency"
    SERVER_OFFLINE = "server_offline"
    MACHINE_BUSY = "machine_busy"


class OperatorConfig(BaseModel):
    id: str
    name: str
    color: str
    storage_mode: StorageMode


class SiteConfig(BaseModel):
    id: str
    name: str
    operator_id: str
    storage_mode: StorageMode
    sync_interval_sec: float = 10.0


class MachineConfig(BaseModel):
    id: str
    site_id: str
    name: str
    game_type: str
    credit_cost: int = Field(ge=1)
    pi_id: str
    slot_index: int


class IssueCardRequest(BaseModel):
    site_id: str
    customer_name: str = "Walk-in Player"
    initial_credits: int = Field(default=10, ge=0, le=10000)
    card_uid: Optional[str] = None


class TopUpRequest(BaseModel):
    site_id: str
    card_uid: str
    credits: int = Field(gt=0, le=10000)
    idempotency_key: Optional[str] = None


class PiTapRequest(BaseModel):
    card_uid: str
    idempotency_key: Optional[str] = None
    bypass_debounce: bool = False


class ServerDeductRequest(BaseModel):
    card_uid: str
    site_id: str
    operator_id: str
    machine_id: str
    pi_id: str
    credit_cost: int = Field(ge=1)
    idempotency_key: str


class ServerDeductResponse(BaseModel):
    approved: bool
    outcome: TapOutcome
    message: str
    card_uid: str
    operator_id: Optional[str] = None
    site_id: str
    machine_id: str
    pi_id: str
    credits_deducted: int = 0
    remaining_balance: Optional[int] = None
    storage_mode: StorageMode
    latency_ms: float = 0.0
    tx_id: Optional[str] = None


class SimulationSettings(BaseModel):
    running: bool = True
    speed: float = Field(default=1.0, ge=0.25, le=10.0)
    cloud_outage: bool = False
    cloud_latency_ms: int = Field(default=45, ge=0, le=3000)
    local_latency_ms: int = Field(default=8, ge=0, le=1000)
    local_sync_interval_sec: float = Field(default=10.0, ge=2.0, le=120.0)
    pi_debounce_ms: int = Field(default=1200, ge=0, le=10000)


class SimulationSettingsPatch(BaseModel):
    running: Optional[bool] = None
    speed: Optional[float] = Field(default=None, ge=0.25, le=10.0)
    cloud_outage: Optional[bool] = None
    cloud_latency_ms: Optional[int] = Field(default=None, ge=0, le=3000)
    local_latency_ms: Optional[int] = Field(default=None, ge=0, le=1000)
    local_sync_interval_sec: Optional[float] = Field(default=None, ge=2.0, le=120.0)
    pi_debounce_ms: Optional[int] = Field(default=None, ge=0, le=10000)


class SpawnCustomerRequest(BaseModel):
    site_id: Optional[str] = None
    customer_name: Optional[str] = None
    initial_credits: int = Field(default=12, ge=1, le=500)
    behavior: Literal["normal", "roam_same_op", "cross_op_attempt", "double_tap"] = "normal"


class StressTestRequest(BaseModel):
    total_requests: int = Field(default=500, ge=10, le=20000)
    concurrency: int = Field(default=50, ge=1, le=500)
    num_cards: int = Field(default=40, ge=1, le=1000)
    initial_credits_per_card: int = Field(default=25, ge=1, le=10000)
    cross_operator_ratio: float = Field(default=0.10, ge=0.0, le=0.5)
    double_tap_ratio: float = Field(default=0.10, ge=0.0, le=0.5)
    target_mode: Literal["mixed", "cloud", "local"] = "mixed"
    bypass_debounce: bool = True
