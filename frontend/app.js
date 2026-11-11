/* Arcade NFC Simulation Canvas & Dashboard Controller */

const canvas = document.getElementById("arcade-canvas");
const ctx = canvas.getContext("2d");

let latestSnapshot = null;
let packets = []; // Animated HTTP pulses between Pi <-> Server
let siteBoxes = {}; // Computed canvas bounding boxes per site_id
let cloudHubPos = { x: 0, y: 0 };
let ws = null;

// Normalized coordinates matching backend/simulator.py
const SITE_POS = {
  kiosk: { x: 0.16, y: 0.54 },
  entrance: { x: 0.16, y: 0.88 },
  server: { x: 0.16, y: 0.20 },
  slots: [
    { x: 0.45, y: 0.34 },
    { x: 0.77, y: 0.34 },
    { x: 0.45, y: 0.72 },
    { x: 0.77, y: 0.72 },
  ],
};

// ------------------------------------------------------------------
// WebSocket & API Helpers
// ------------------------------------------------------------------
function connectWebSocket() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${proto}//${location.host}/ws/events`);
  ws.onmessage = (msg) => {
    const data = JSON.parse(msg.data);
    if (data.type === "snapshot" || data.type === "reset") {
      latestSnapshot = data.snapshot;
      updateTelemetryUI(latestSnapshot);
      if (data.type === "reset") {
        document.getElementById("event-log").innerHTML = "";
        refreshAccounts();
      }
    } else if (data.type === "tap_result") {
      if (latestSnapshot && latestSnapshot.metrics) {
        latestSnapshot.metrics.total_taps += 1;
        if (data.outcome && Object.prototype.hasOwnProperty.call(latestSnapshot.metrics, data.outcome)) {
          latestSnapshot.metrics[data.outcome] += 1;
        }
        updateTelemetryUI(latestSnapshot);
      }
      spawnPacketFromTap(data);
      prependLogItem(formatTapLog(data), data.approved ? "ok" : data.outcome === "debounced" ? "warn" : "err");
      refreshAccounts();
    } else if (data.type === "topup_event") {
      prependLogItem(
        `[KIOSK ${data.site_id}] Issued/Top-up ${data.card_uid} -> ${data.balance} CR (${data.storage_mode.toUpperCase()})`,
        "ok"
      );
      refreshAccounts();
    } else if (data.type === "sync_event") {
      spawnSyncPulses(data);
      prependLogItem(
        `[LOCAL SYNC] ${data.operator_id}: synced ${data.sites_synced} sites (${data.entries_replicated} ledger entries replicated)`,
        "sync"
      );
      refreshAccounts();
    }
  };
  ws.onclose = () => {
    setTimeout(connectWebSocket, 1500);
  };
}

async function apiPatchSettings(patch) {
  await fetch("/api/sim/settings", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
}

async function spawnScenario(behavior) {
  await fetch("/api/sim/spawn", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ behavior, initial_credits: 10 }),
  });
}
window.spawnScenario = spawnScenario;

async function refreshAccounts() {
  try {
    const res = await fetch("/api/sim/accounts");
    const data = await res.json();
    const accounts = data.accounts || [];
    document.getElementById("account-count").textContent = accounts.length;

    const tbody = document.getElementById("accounts-tbody");
    tbody.innerHTML = accounts
      .slice(0, 60)
      .map((a) => {
        const opColor = a.operator_id === "op-neon" ? "#38bdf8" : "#f472b6";
        let balText = `${a.balance} CR`;
        if (a.site_balances) {
          const vals = Object.entries(a.site_balances)
            .map(([s, b]) => `${s.replace("site-retro-", "")}:${b}`)
            .join(" / ");
          balText = `${a.balance} CR (${vals})`;
        }
        return `<tr>
          <td><code>${a.card_uid}</code></td>
          <td style="color:${opColor}">${a.operator_id}</td>
          <td>${a.storage_mode}</td>
          <td><strong>${balText}</strong></td>
        </tr>`;
      })
      .join("");

    // Update manual card selector preserving selection
    const cardSel = document.getElementById("manual-card-select");
    const prev = cardSel.value;
    cardSel.innerHTML = accounts
      .map(
        (a) =>
          `<option value="${a.card_uid}">${a.card_uid} (${a.operator_id}, ${a.balance} CR)</option>`
      )
      .join("");
    if (prev && [...cardSel.options].some((o) => o.value === prev)) {
      cardSel.value = prev;
    }
  } catch (_) {}
}

function updateTelemetryUI(snap) {
  if (!snap) return;
  const m = snap.metrics;
  document.getElementById("stat-total").textContent = m.total_taps;
  document.getElementById("stat-approved").textContent = m.approved;
  document.getElementById("stat-insufficient").textContent = m.insufficient_credits;
  document.getElementById("stat-mismatch").textContent = m.operator_mismatch;
  document.getElementById("stat-latency").textContent = `${m.avg_latency_ms} / ${m.p95_latency_ms} ms`;

  const s = snap.settings;
  const runBtn = document.getElementById("btn-toggle-run");
  runBtn.textContent = s.running ? "Pause" : "Resume";

  const cloudPill = document.getElementById("cloud-status-pill");
  const cloudVal = document.getElementById("stat-cloud");
  const outageBtn = document.getElementById("btn-cloud-outage");
  if (s.cloud_outage) {
    cloudPill.classList.add("offline");
    cloudVal.textContent = "OUTAGE";
    outageBtn.textContent = "Restore Cloud WAN";
    outageBtn.className = "btn danger-solid";
  } else {
    cloudPill.classList.remove("offline");
    cloudVal.textContent = "ONLINE";
    outageBtn.textContent = "Simulate Cloud Outage";
    outageBtn.className = "btn danger-outline";
  }

  // Populate machine dropdown once
  const machSel = document.getElementById("manual-machine-select");
  if (machSel.options.length === 0 && snap.machines) {
    machSel.innerHTML = snap.machines
      .map(
        (mach) =>
          `<option value="${mach.pi_id}" data-site="${mach.site_id}">${mach.site_id} -> ${mach.name} (${mach.credit_cost} CR)</option>`
      )
      .join("");
  }
}

function formatTapLog(evt) {
  const mode = (evt.storage_mode || "").toUpperCase();
  const bal = evt.remaining_balance !== null && evt.remaining_balance !== undefined ? `bal=${evt.remaining_balance}` : "";
  return `[${evt.pi_id} -> ${mode}] ${evt.outcome.toUpperCase()} card=${evt.card_uid} cost=${evt.credits_deducted} ${bal} (${evt.latency_ms}ms)`;
}

function prependLogItem(text, cls) {
  const log = document.getElementById("event-log");
  const div = document.createElement("div");
  div.className = `log-item ${cls}`;
  div.textContent = `${new Date().toLocaleTimeString()} ${text}`;
  log.prepend(div);
  while (log.children.length > 60) {
    log.removeChild(log.lastChild);
  }
}

// ------------------------------------------------------------------
// Animated Network Packet Pulses
// ------------------------------------------------------------------
function spawnPacketFromTap(evt) {
  if (!latestSnapshot) return;
  const mach = latestSnapshot.machines.find((m) => m.pi_id === evt.pi_id);
  const box = siteBoxes[evt.site_id];
  if (!mach || !box) return;

  const slot = SITE_POS.slots[mach.slot_index % 4];
  const from = {
    x: box.x + slot.x * box.w,
    y: box.y + slot.y * box.h - 14,
  };
  const to =
    evt.storage_mode === "cloud"
      ? { x: cloudHubPos.x, y: cloudHubPos.y }
      : { x: box.x + SITE_POS.server.x * box.w, y: box.y + SITE_POS.server.y * box.h };

  const color = evt.approved ? "#22c55e" : evt.outcome === "debounced" ? "#f59e0b" : "#ef4444";
  packets.push({ from, to, color, progress: 0, roundTrip: true });
}

function spawnSyncPulses(evt) {
  const b1 = siteBoxes["site-retro-north"];
  const b2 = siteBoxes["site-retro-south"];
  if (!b1 || !b2) return;
  const p1 = { x: b1.x + SITE_POS.server.x * b1.w, y: b1.y + SITE_POS.server.y * b1.h };
  const p2 = { x: b2.x + SITE_POS.server.x * b2.w, y: b2.y + SITE_POS.server.y * b2.h };
  packets.push({ from: p1, to: p2, color: "#a855f7", progress: 0, roundTrip: true });
}

// ------------------------------------------------------------------
// HTML5 Canvas Renderer
// ------------------------------------------------------------------
function resizeCanvas() {
  const wrap = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = wrap.clientWidth * dpr;
  canvas.height = wrap.clientHeight * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
window.addEventListener("resize", resizeCanvas);
resizeCanvas();

function drawRoundedRect(x, y, w, h, r, fill, stroke) {
  ctx.beginPath();
  ctx.roundRect(x, y, w, h, r);
  if (fill) {
    ctx.fillStyle = fill;
    ctx.fill();
  }
  if (stroke) {
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 1.5;
    ctx.stroke();
  }
}

function renderLoop() {
  const w = canvas.parentElement.clientWidth;
  const h = canvas.parentElement.clientHeight;
  ctx.clearRect(0, 0, w, h);

  if (latestSnapshot) {
    drawScene(w, h, latestSnapshot);
  } else {
    ctx.fillStyle = "#94a3b8";
    ctx.font = "14px Inter, sans-serif";
    ctx.fillText("Connecting to Arcade Simulation Server...", 24, 36);
  }
  requestAnimationFrame(renderLoop);
}

function drawScene(w, h, snap) {
  const pad = 16;
  const cloudBarH = 44;
  cloudHubPos = { x: w / 2, y: pad + 20 };

  // Draw Top Cloud Server Hub
  const cloudOnline = !snap.settings.cloud_outage;
  drawRoundedRect(
    w / 2 - 155,
    pad,
    310,
    38,
    10,
    cloudOnline ? "rgba(56, 189, 248, 0.14)" : "rgba(239, 68, 68, 0.18)",
    cloudOnline ? "#38bdf8" : "#ef4444"
  );
  ctx.fillStyle = cloudOnline ? "#e0f2fe" : "#fecaca";
  ctx.font = "bold 12px Inter, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(
    cloudOnline
      ? `CENTRAL CLOUD SERVER (ONLINE • ${snap.settings.cloud_latency_ms}ms)`
      : "CENTRAL CLOUD SERVER (OFFLINE / WAN OUTAGE)",
    w / 2,
    pad + 23
  );

  // 2x2 Grid for the 4 Sites
  const gridTop = pad + cloudBarH + 14;
  const gridW = (w - pad * 3) / 2;
  const gridH = (h - gridTop - pad * 2) / 2;

  const siteOrder = [
    "site-neon-downtown",
    "site-neon-mall",
    "site-retro-north",
    "site-retro-south",
  ];

  siteOrder.forEach((siteId, idx) => {
    const col = idx % 2;
    const row = Math.floor(idx / 2);
    const sx = pad + col * (gridW + pad);
    const sy = gridTop + row * (gridH + pad);
    siteBoxes[siteId] = { x: sx, y: sy, w: gridW, h: gridH };

    const site = snap.sites.find((s) => s.id === siteId);
    if (!site) return;
    const isCloud = site.storage_mode === "cloud";
    const accent = isCloud ? "#38bdf8" : "#f472b6";

    // Draw Site Container
    drawRoundedRect(sx, sy, gridW, gridH, 12, "#0f172a", site.online ? accent : "#ef4444");

    // Site Header
    ctx.textAlign = "left";
    ctx.fillStyle = "#f8fafc";
    ctx.font = "bold 13px Inter, sans-serif";
    ctx.fillText(site.name, sx + 14, sy + 22);

    ctx.textAlign = "right";
    ctx.font = "bold 11px Inter, sans-serif";
    if (isCloud) {
      ctx.fillStyle = site.online ? "#38bdf8" : "#ef4444";
      ctx.fillText(
        site.online ? "CLOUD TIER • REAL-TIME" : "CLOUD OFFLINE (BLOCKED)",
        sx + gridW - 14,
        sy + 22
      );
    } else {
      ctx.fillStyle = "#f472b6";
      const syncLabel = site.wan_online
        ? `LOCAL TIER • SYNC IN ${site.next_sync_in_sec}s (PENDING: ${site.pending_sync_count})`
        : `LOCAL ACTIVE (WAN DOWN • PENDING: ${site.pending_sync_count})`;
      ctx.fillText(syncLabel, sx + gridW - 14, sy + 22);
    }

    // Draw Cloud Link line or Local Server Hub
    const srvX = sx + SITE_POS.server.x * gridW;
    const srvY = sy + SITE_POS.server.y * gridH + 8;

    if (isCloud) {
      ctx.beginPath();
      ctx.setLineDash([4, 4]);
      ctx.moveTo(srvX, srvY);
      ctx.lineTo(cloudHubPos.x, cloudHubPos.y + 18);
      ctx.strokeStyle = site.online ? "rgba(56,189,248,0.35)" : "rgba(239,68,68,0.45)";
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.setLineDash([]);

      drawRoundedRect(srvX - 42, srvY - 14, 84, 28, 6, "#1e293b", site.online ? "#38bdf8" : "#ef4444");
      ctx.textAlign = "center";
      ctx.fillStyle = "#cbd5e1";
      ctx.font = "10px Inter, sans-serif";
      ctx.fillText("WAN Router", srvX, srvY + 4);
    } else {
      drawRoundedRect(srvX - 48, srvY - 14, 96, 28, 6, "#1e293b", "#f472b6");
      ctx.textAlign = "center";
      ctx.fillStyle = "#fbcfe8";
      ctx.font = "bold 10px Inter, sans-serif";
      ctx.fillText("LOCAL SERVER", srvX, srvY + 4);
    }

    // Draw Kiosk
    const kx = sx + SITE_POS.kiosk.x * gridW;
    const ky = sy + SITE_POS.kiosk.y * gridH;
    drawRoundedRect(kx - 42, ky - 22, 84, 44, 8, "#172554", "#60a5fa");
    ctx.textAlign = "center";
    ctx.fillStyle = "#dbeafe";
    ctx.font = "bold 10px Inter, sans-serif";
    ctx.fillText("TOP-UP KIOSK", kx, ky - 2);
    ctx.fillStyle = "#93c5fd";
    ctx.font = "9px Inter, sans-serif";
    ctx.fillText("Buy / Reload", kx, ky + 12);

    // Draw Entrance
    const ex = sx + SITE_POS.entrance.x * gridW;
    const ey = sy + SITE_POS.entrance.y * gridH;
    ctx.fillStyle = "rgba(148,163,184,0.25)";
    ctx.fillRect(ex - 28, ey + 6, 56, 4);
    ctx.fillStyle = "#64748b";
    ctx.font = "9px Inter, sans-serif";
    ctx.fillText("ENTRANCE", ex, ey + 2);

    // Draw Machines + Raspberry Pis
    const siteMachines = snap.machines.filter((m) => m.site_id === siteId);
    siteMachines.forEach((m) => {
      const slot = SITE_POS.slots[m.slot_index % 4];
      const mx = sx + slot.x * gridW;
      const my = sy + slot.y * gridH;
      const playing = m.status === "playing";

      drawRoundedRect(
        mx - 58,
        my - 26,
        116,
        52,
        8,
        playing ? "rgba(34, 197, 94, 0.16)" : "#1e293b",
        playing ? "#22c55e" : "#334155"
      );

      ctx.textAlign = "left";
      ctx.fillStyle = "#f1f5f9";
      ctx.font = "bold 10.5px Inter, sans-serif";
      ctx.fillText(m.name, mx - 50, my - 8);

      ctx.fillStyle = "#94a3b8";
      ctx.font = "9.5px Inter, sans-serif";
      ctx.fillText(`${m.game_type} • ${m.credit_cost} CR`, mx - 50, my + 6);
      ctx.fillText(`Plays: ${m.total_plays}`, mx - 50, my + 19);

      // Raspberry Pi NFC Reader badge + LED
      const ledColors = {
        ready: "#38bdf8",
        green: "#22c55e",
        amber: "#f59e0b",
        red: "#ef4444",
      };
      const ledColor = ledColors[m.pi_led] || "#38bdf8";
      ctx.beginPath();
      ctx.arc(mx + 44, my - 12, 5, 0, Math.PI * 2);
      ctx.fillStyle = ledColor;
      ctx.fill();
      ctx.textAlign = "right";
      ctx.fillStyle = "#cbd5e1";
      ctx.font = "8.5px monospace";
      ctx.fillText("Pi NFC", mx + 52, my + 19);
    });

    // Draw Customers inside this site
    const siteCustomers = snap.customers.filter((c) => c.site_id === siteId);
    siteCustomers.forEach((c) => {
      const cx = sx + c.x * gridW;
      const cy = sy + c.y * gridH;

      // Avatar circle
      ctx.beginPath();
      ctx.arc(cx, cy, 9, 0, Math.PI * 2);
      ctx.fillStyle = c.color;
      ctx.fill();
      ctx.lineWidth = 2;
      ctx.strokeStyle = "#f8fafc";
      ctx.stroke();

      // Label below
      ctx.textAlign = "center";
      ctx.fillStyle = "#e2e8f0";
      ctx.font = "bold 9px Inter, sans-serif";
      ctx.fillText(`${c.name.split(" ")[0]} (${c.known_balance}c)`, cx, cy + 19);

      // Floating status bubble
      if (c.bubble_text) {
        ctx.font = "bold 9.5px Inter, sans-serif";
        const tw = ctx.measureText(c.bubble_text).width + 12;
        drawRoundedRect(cx - tw / 2, cy - 30, tw, 17, 5, "#0f172a", c.bubble_color);
        ctx.fillStyle = c.bubble_color;
        ctx.fillText(c.bubble_text, cx, cy - 18);
      }
    });
  });

  // Peer Local Sync Link indicator between RetroVault North & South
  const bNorth = siteBoxes["site-retro-north"];
  const bSouth = siteBoxes["site-retro-south"];
  if (bNorth && bSouth) {
    const p1x = bNorth.x + SITE_POS.server.x * bNorth.w + 48;
    const p1y = bNorth.y + SITE_POS.server.y * bNorth.h + 8;
    const p2x = bSouth.x + SITE_POS.server.x * bSouth.w - 48;
    const p2y = bSouth.y + SITE_POS.server.y * bSouth.h + 8;
    ctx.beginPath();
    ctx.setLineDash([5, 5]);
    ctx.moveTo(p1x, p1y);
    ctx.lineTo(p2x, p2y);
    ctx.strokeStyle = snap.settings.cloud_outage ? "rgba(239,68,68,0.4)" : "rgba(168,85,247,0.45)";
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // Draw & advance animated HTTP packets
  packets = packets.filter((p) => {
    p.progress += 0.055;
    if (p.progress > 1.0) return false;
    const t = p.roundTrip ? (p.progress < 0.5 ? p.progress * 2 : (1 - p.progress) * 2) : p.progress;
    const px = p.from.x + (p.to.x - p.from.x) * t;
    const py = p.from.y + (p.to.y - p.from.y) * t;

    ctx.beginPath();
    ctx.arc(px, py, 4.5, 0, Math.PI * 2);
    ctx.fillStyle = p.color;
    ctx.fill();
    ctx.shadowColor = p.color;
    ctx.shadowBlur = 10;
    ctx.fill();
    ctx.shadowBlur = 0;
    return true;
  });
}

// ------------------------------------------------------------------
// UI Wiring
// ------------------------------------------------------------------
document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach((c) => c.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(`tab-${btn.dataset.tab}`).classList.add("active");
    if (btn.dataset.tab === "accounts") refreshAccounts();
  });
});

document.getElementById("btn-toggle-run").addEventListener("click", async () => {
  if (!latestSnapshot) return;
  await apiPatchSettings({ running: !latestSnapshot.settings.running });
});

document.getElementById("btn-sync-now").addEventListener("click", async () => {
  await fetch("/api/sim/sync-local", { method: "POST" });
});

document.getElementById("btn-cloud-outage").addEventListener("click", async () => {
  if (!latestSnapshot) return;
  await apiPatchSettings({ cloud_outage: !latestSnapshot.settings.cloud_outage });
});

document.getElementById("btn-reset").addEventListener("click", async () => {
  await fetch("/api/sim/reset", { method: "POST" });
});

function bindSlider(inputId, valId, suffix, field, parser = parseInt) {
  const inp = document.getElementById(inputId);
  const val = document.getElementById(valId);
  inp.addEventListener("input", () => {
    val.textContent = `${inp.value}${suffix}`;
  });
  inp.addEventListener("change", () => {
    apiPatchSettings({ [field]: parser(inp.value) });
  });
}

bindSlider("inp-speed", "val-speed", "x", "speed", parseFloat);
bindSlider("inp-cloud-lat", "val-cloud-lat", " ms", "cloud_latency_ms", parseInt);
bindSlider("inp-local-lat", "val-local-lat", " ms", "local_latency_ms", parseInt);
bindSlider("inp-sync-int", "val-sync-int", " s", "local_sync_interval_sec", parseFloat);
bindSlider("inp-debounce", "val-debounce", " ms", "pi_debounce_ms", parseInt);

// Manual Tester Buttons
document.getElementById("btn-manual-tap").addEventListener("click", async () => {
  const card_uid = document.getElementById("manual-card-select").value;
  const pi_id = document.getElementById("manual-machine-select").value;
  if (!card_uid || !pi_id) return;
  const res = await fetch(`/api/pi/${pi_id}/tap`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ card_uid }),
  });
  const data = await res.json();
  document.getElementById("manual-result").textContent = JSON.stringify(data, null, 2);
});

document.getElementById("btn-manual-race").addEventListener("click", async () => {
  const card_uid = document.getElementById("manual-card-select").value;
  const sel = document.getElementById("manual-machine-select");
  const site_id = sel.options[sel.selectedIndex]?.dataset?.site;
  if (!card_uid || !site_id) return;
  const res = await fetch(`/api/sim/double-tap-demo?site_id=${encodeURIComponent(site_id)}&card_uid=${encodeURIComponent(card_uid)}`, {
    method: "POST",
  });
  const data = await res.json();
  document.getElementById("manual-result").textContent = JSON.stringify(data, null, 2);
});

document.getElementById("btn-manual-topup").addEventListener("click", async () => {
  const card_uid = document.getElementById("manual-card-select").value;
  const sel = document.getElementById("manual-machine-select");
  const site_id = sel.options[sel.selectedIndex]?.dataset?.site;
  if (!card_uid || !site_id) return;
  const res = await fetch("/api/kiosk/topup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ site_id, card_uid, credits: 10 }),
  });
  const data = await res.json();
  document.getElementById("manual-result").textContent = JSON.stringify(data, null, 2);
  refreshAccounts();
});

document.getElementById("btn-issue-card").addEventListener("click", async () => {
  const sel = document.getElementById("manual-machine-select");
  const site_id = sel.options[sel.selectedIndex]?.dataset?.site || "site-neon-downtown";
  const res = await fetch("/api/kiosk/issue-card", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ site_id, customer_name: "Manual Test Player", initial_credits: 10 }),
  });
  const data = await res.json();
  document.getElementById("manual-result").textContent = JSON.stringify(data, null, 2);
  await refreshAccounts();
  if (data.card_uid) {
    document.getElementById("manual-card-select").value = data.card_uid;
  }
});

// Stress Test Runner
document.getElementById("btn-run-stress").addEventListener("click", async () => {
  const btn = document.getElementById("btn-run-stress");
  const out = document.getElementById("stress-results");
  btn.disabled = true;
  btn.textContent = "Running Stress Test...";
  out.innerHTML = "<p class='muted'>Executing concurrent HTTP load & auditing invariants...</p>";

  const payload = {
    total_requests: parseInt(document.getElementById("st-total").value, 10),
    concurrency: parseInt(document.getElementById("st-concurrency").value, 10),
    num_cards: parseInt(document.getElementById("st-cards").value, 10),
    initial_credits_per_card: parseInt(document.getElementById("st-credits").value, 10),
    cross_operator_ratio: parseFloat(document.getElementById("st-cross").value) / 100.0,
    double_tap_ratio: parseFloat(document.getElementById("st-double").value) / 100.0,
    target_mode: document.getElementById("st-mode").value,
    bypass_debounce: true,
  };

  try {
    const res = await fetch("/api/stress-test/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const r = await res.json();
    const outcomesHtml = Object.entries(r.outcomes || {})
      .map(([k, v]) => `<div>${k}: <strong>${v}</strong></div>`)
      .join("");

    out.innerHTML = `
      <div class="audit-banner ${r.passed ? "pass" : "fail"}">
        ${r.passed ? "INVARIANTS PASSED • ZERO DOUBLE-SPEND / LEAKS" : "AUDIT FAILED"}
      </div>
      <div class="stress-grid">
        <div class="stress-metric"><div class="k">Throughput</div><div class="v">${r.throughput_rps} req/s</div></div>
        <div class="stress-metric"><div class="k">Elapsed</div><div class="v">${r.elapsed_sec} s</div></div>
        <div class="stress-metric"><div class="k">Latency p50 / p95</div><div class="v">${r.latency_ms.p50} / ${r.latency_ms.p95} ms</div></div>
        <div class="stress-metric"><div class="k">Latency p99 / max</div><div class="v">${r.latency_ms.p99} / ${r.latency_ms.max} ms</div></div>
        <div class="stress-metric"><div class="k">Negative Balances</div><div class="v">${r.audit.negative_balance_count}</div></div>
        <div class="stress-metric"><div class="k">Cross-Op Leaks</div><div class="v">${r.audit.cross_operator_leak_count}</div></div>
        <div class="stress-metric"><div class="k">Cloud Conserved</div><div class="v">${r.audit.cloud.conserved ? "YES" : "NO"} (${r.audit.cloud.topped_up}-${r.audit.cloud.deducted}=${r.audit.cloud.remaining_balance})</div></div>
        <div class="stress-metric"><div class="k">Local Conserved</div><div class="v">${r.audit.local.conserved ? "YES" : "NO"} (${r.audit.local.topped_up}-${r.audit.local.deducted}=${r.audit.local.remaining_balance})</div></div>
      </div>
      <div class="manual-result" style="margin-top:8px">${outcomesHtml}</div>
    `;
    refreshAccounts();
  } catch (err) {
    out.innerHTML = `<p style="color:#ef4444">Stress test failed: ${err}</p>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Stress Test";
  }
});

connectWebSocket();
refreshAccounts();
setInterval(refreshAccounts, 4000);
requestAnimationFrame(renderLoop);
