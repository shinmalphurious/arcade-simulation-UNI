"""Default arcade topology: Operators, Sites, Machines, and Raspberry Pi NFC Readers."""

from __future__ import annotations

from backend.models import MachineConfig, OperatorConfig, SiteConfig, StorageMode

DEFAULT_OPERATORS: list[OperatorConfig] = [
    OperatorConfig(
        id="op-neon",
        name="NeonPlay Group",
        color="#38bdf8",
        storage_mode=StorageMode.CLOUD,
    ),
    OperatorConfig(
        id="op-retro",
        name="RetroVault Inc.",
        color="#f472b6",
        storage_mode=StorageMode.LOCAL,
    ),
]

DEFAULT_SITES: list[SiteConfig] = [
    SiteConfig(
        id="site-neon-downtown",
        name="NeonPlay Downtown",
        operator_id="op-neon",
        storage_mode=StorageMode.CLOUD,
        sync_interval_sec=0.0,
    ),
    SiteConfig(
        id="site-neon-mall",
        name="NeonPlay Westside Mall",
        operator_id="op-neon",
        storage_mode=StorageMode.CLOUD,
        sync_interval_sec=0.0,
    ),
    SiteConfig(
        id="site-retro-north",
        name="RetroVault North",
        operator_id="op-retro",
        storage_mode=StorageMode.LOCAL,
        sync_interval_sec=10.0,
    ),
    SiteConfig(
        id="site-retro-south",
        name="RetroVault South",
        operator_id="op-retro",
        storage_mode=StorageMode.LOCAL,
        sync_interval_sec=10.0,
    ),
]

DEFAULT_MACHINES: list[MachineConfig] = [
    # NeonPlay Downtown (Cloud)
    MachineConfig(id="m-nd-1", site_id="site-neon-downtown", name="Cyber Drift X", game_type="Racing", credit_cost=2, pi_id="pi-nd-1", slot_index=0),
    MachineConfig(id="m-nd-2", site_id="site-neon-downtown", name="Pixel Striker", game_type="Fighter", credit_cost=1, pi_id="pi-nd-2", slot_index=1),
    MachineConfig(id="m-nd-3", site_id="site-neon-downtown", name="VR Mech Arena", game_type="VR Sim", credit_cost=4, pi_id="pi-nd-3", slot_index=2),
    MachineConfig(id="m-nd-4", site_id="site-neon-downtown", name="Claw Master", game_type="Prize", credit_cost=3, pi_id="pi-nd-4", slot_index=3),
    # NeonPlay Westside Mall (Cloud)
    MachineConfig(id="m-nm-1", site_id="site-neon-mall", name="Hyper Rhythm", game_type="Rhythm", credit_cost=2, pi_id="pi-nm-1", slot_index=0),
    MachineConfig(id="m-nm-2", site_id="site-neon-mall", name="Galaxy Blaster", game_type="Shooter", credit_cost=1, pi_id="pi-nm-2", slot_index=1),
    MachineConfig(id="m-nm-3", site_id="site-neon-mall", name="Apex Rally", game_type="Racing", credit_cost=3, pi_id="pi-nm-3", slot_index=2),
    MachineConfig(id="m-nm-4", site_id="site-neon-mall", name="Mega Hoops", game_type="Sports", credit_cost=2, pi_id="pi-nm-4", slot_index=3),
    # RetroVault North (Local)
    MachineConfig(id="m-rn-1", site_id="site-retro-north", name="Pac-Legend 82", game_type="Classic", credit_cost=1, pi_id="pi-rn-1", slot_index=0),
    MachineConfig(id="m-rn-2", site_id="site-retro-north", name="Pinball Wizard", game_type="Pinball", credit_cost=2, pi_id="pi-rn-2", slot_index=1),
    MachineConfig(id="m-rn-3", site_id="site-retro-north", name="Street Brawler", game_type="Fighter", credit_cost=1, pi_id="pi-rn-3", slot_index=2),
    MachineConfig(id="m-rn-4", site_id="site-retro-north", name="Air Hockey Pro", game_type="Table", credit_cost=3, pi_id="pi-rn-4", slot_index=3),
    # RetroVault South (Local)
    MachineConfig(id="m-rs-1", site_id="site-retro-south", name="Space Invade", game_type="Classic", credit_cost=1, pi_id="pi-rs-1", slot_index=0),
    MachineConfig(id="m-rs-2", site_id="site-retro-south", name="Neon Pinball", game_type="Pinball", credit_cost=2, pi_id="pi-rs-2", slot_index=1),
    MachineConfig(id="m-rs-3", site_id="site-retro-south", name="Turbo Outrun", game_type="Racing", credit_cost=2, pi_id="pi-rs-3", slot_index=2),
    MachineConfig(id="m-rs-4", site_id="site-retro-south", name="Dino Blaster", game_type="Shooter", credit_cost=3, pi_id="pi-rs-4", slot_index=3),
]
