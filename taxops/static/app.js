// ── SEC-1: CSRF-aware fetch helper ─────────────────────────────────────────
// Reads the token from <meta name="csrf-token"> (injected by base.html / login.html).
// All mutating requests (POST / PATCH / PUT / DELETE) automatically get X-CSRFToken.
// Read-only methods (GET / HEAD) are passed through unchanged.
function _csrfToken() {
  const m = document.querySelector('meta[name="csrf-token"]');
  return m ? m.getAttribute("content") : "";
}

function _csrfFetch(url, options) {
  const opts = Object.assign({}, options || {});
  const method = ((opts.method || "GET").toUpperCase());
  const mutating = method === "POST" || method === "PATCH" || method === "PUT" || method === "DELETE";
  if (mutating) {
    opts.headers = Object.assign({ "X-CSRFToken": _csrfToken() }, opts.headers || {});
  }
  return fetch(url, opts);
}

// ── Status badge Tailwind classes (mirrors app.py STATUS_BADGE) ───────────
const STATUS_BADGE = {
  "PROCESSING":  "bg-sky-50 text-sky-700 border-sky-200",
  "FINALIZE":    "bg-yellow-50 text-yellow-700 border-yellow-200",
  "PICKUP":      "bg-teal-50 text-teal-700 border-teal-200",
  "EFILE READY": "bg-indigo-50 text-indigo-700 border-indigo-200",
  "LOG OUT":     "bg-slate-100 text-slate-500 border-slate-200",
  "REJECTED":    "bg-red-50 text-red-700 border-red-200",
};

const STATUS_ROW = {
  "PROCESSING":  "status-PROCESSING",
  "FINALIZE":    "status-FINALIZE",
  "PICKUP":      "status-PICKUP",
  "EFILE READY": "status-EFILE-READY",
  "LOG OUT":     "status-LOG-OUT",
  "REJECTED":    "status-REJECTED",
};

// ── Live global search ─────────────────────────────────────────────────────

let _searchTimer = null;

function initSearch() {
  const input   = document.getElementById("global-search");
  const results = document.getElementById("search-results");
  if (!input) return;

  input.addEventListener("input", () => {
    clearTimeout(_searchTimer);
    const q = input.value.trim();
    if (!q) { results.classList.add("hidden"); return; }
    _searchTimer = setTimeout(() => runSearch(q), 160);
  });

  input.addEventListener("keydown", e => {
    if (e.key === "Escape") {
      results.classList.add("hidden");
      input.value = "";
    }
  });

  document.addEventListener("click", e => {
    if (!input.contains(e.target) && !results.contains(e.target)) {
      results.classList.add("hidden");
    }
  });
}

async function runSearch(q) {
  const year    = document.body.dataset.year || new Date().getFullYear();
  const results = document.getElementById("search-results");
  try {
    const resp = await fetch(`/api/search?q=${encodeURIComponent(q)}&year=${year}`);
    const data = await resp.json();
    renderSearchResults(data, results);
  } catch { /* network error – silently ignore */ }
}

function renderSearchResults(items, container) {
  if (!items.length) {
    container.innerHTML = `<div class="px-4 py-3 text-sm text-slate-400">No results</div>`;
    container.classList.remove("hidden");
    return;
  }
  container.innerHTML = items.map(r => {
    const href = (r.client_id != null && r.client_id !== "") ? `/clients/${r.client_id}` : `/return/${r.id}`;
    return `
    <a href="${href}"
       class="flex items-center gap-3 px-4 py-2.5 hover:bg-slate-50 transition-colors border-b border-slate-100 last:border-0">
      <span class="font-mono font-bold text-slate-400 w-10 shrink-0 text-xs">${r.log_number ?? '—'}</span>
      <span class="flex-1 min-w-0">
        <span class="block text-sm font-semibold text-slate-800">${r.name}</span>
        <span class="text-xs text-slate-400">TY${r.tax_year ?? '—'}</span>
      </span>
      <span class="text-xs px-2 py-px rounded-full border shrink-0 ${r.badge || "bg-slate-100 text-slate-500 border-slate-200"}">${r.status || "—"}</span>
    </a>
  `;
  }).join("");
  container.classList.remove("hidden");
}

// ── Status dropdown (dashboard) ─────────────────────────────────────────────

function toggleStatusMenu(btn) {
  const menu   = btn.nextElementSibling;
  const hidden = menu.classList.contains("hidden");
  // close all open menus first
  document.querySelectorAll(".status-menu").forEach(m => m.classList.add("hidden"));
  if (hidden) menu.classList.remove("hidden");
}

async function setStatus(returnId, status, btn) {
  const menu  = btn.closest(".status-menu");
  const badge = menu.previousElementSibling;
  try {
    const resp = await _csrfFetch(`/api/return/${returnId}/status`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ status }),
    });
    const data = await resp.json();
    if (!data.success) return;

    // Update badge text + classes
    badge.textContent = data.client_status;
    const base = "status-badge flex items-center gap-1";
    badge.className   = `${base} ${STATUS_BADGE[data.client_status] || "bg-slate-100 text-slate-500 border-slate-200"}`;

    // Re-add chevron icon
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "w-2.5 h-2.5 opacity-40 shrink-0");
    svg.setAttribute("fill", "none");
    svg.setAttribute("stroke", "currentColor");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.innerHTML = `<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 9l-7 7-7-7"/>`;
    badge.appendChild(svg);

    // Update row border color
    const row = badge.closest("tr");
    if (row) {
      Object.values(STATUS_ROW).forEach(c => row.classList.remove(c));
      if (STATUS_ROW[data.client_status]) row.classList.add(STATUS_ROW[data.client_status]);
    }

    // Update active checkmark in menu
    menu.querySelectorAll("button").forEach(b => {
      const isActive = b.textContent.trim().startsWith(data.client_status);
      b.classList.toggle("font-bold", isActive);
      b.classList.toggle("text-slate-900", isActive);
      b.classList.toggle("bg-slate-50", isActive);
    });

    menu.classList.add("hidden");
    flash(badge);
  } catch (e) {
    console.error("Status update failed", e);
  }
}

// ── Preparer: mirror taxops/preparer.py (full names in DB) ─────────────────
const LUCILA_YANEZ = "Lucila Yanez";
const MOISES_BUSTOS = "Moises Bustos";

function preparerListLabelJs(code) {
  if (!code || !String(code).trim()) return "—";
  const raw = String(code).trim();
  const k = raw.toLowerCase().replace(/\s+/g, " ");
  const map = {
    ly: LUCILA_YANEZ,
    "l.y.": LUCILA_YANEZ,
    "l y": LUCILA_YANEZ,
    lucila: LUCILA_YANEZ,
    yanez: LUCILA_YANEZ,
    "lucila yanez": LUCILA_YANEZ,
    mb: MOISES_BUSTOS,
    "m.b.": MOISES_BUSTOS,
    "m b": MOISES_BUSTOS,
    moises: MOISES_BUSTOS,
    bustos: MOISES_BUSTOS,
    "moises bustos": MOISES_BUSTOS,
  };
  if (map[k] !== undefined) return map[k];
  if (raw.length <= 3 && raw === raw.toUpperCase() && /^[A-Z.]+$/.test(raw)) {
    if (raw.replace(/\./g, "") === "LY") return LUCILA_YANEZ;
    if (raw.replace(/\./g, "") === "MB") return MOISES_BUSTOS;
  }
  return raw;
}

/** Restore visible text after cancel or no-op edit (inline fields). */
function inlineFieldDisplay(el, storedValue) {
  if (!storedValue || storedValue === "—") return "—";
  if (el.dataset.field === "processor") return preparerListLabelJs(storedValue);
  return storedValue;
}

// ── Inline cell editing ──────────────────────────────────────────────────────

function initInlineEdit() {
  document.addEventListener("click", e => {
    const el = e.target.closest(".editable");
    if (!el || el.querySelector("input")) return;

    if (el.dataset.type === "bool") {
      toggleBool(el);
    } else {
      startEdit(el);
    }
  });
}

function startEdit(el) {
  const current = el.dataset.value ?? el.textContent.replace(/^\$/, "").replace(/,/g, "").trim();
  el.dataset.original = current;
  el.innerHTML = "";

  const input = document.createElement("input");
  input.value       = current === "—" ? "" : current;
  input.placeholder = el.dataset.placeholder || "";
  input.className   = "w-full min-w-[80px] bg-white border border-blue-400 rounded px-1.5 py-0.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400";

  const save = () => commitEdit(el, input.value.trim());

  input.addEventListener("blur",    save);
  input.addEventListener("keydown", e => {
    if (e.key === "Enter")  { e.preventDefault(); save(); }
    if (e.key === "Escape") { el.textContent = inlineFieldDisplay(el, el.dataset.original); }
  });

  el.appendChild(input);
  input.focus();
  input.select();
}

async function commitEdit(el, value) {
  if (!el.querySelector("input")) return; // already committed
  const returnId = el.dataset.returnId;
  const field    = el.dataset.field;
  const original = el.dataset.original;

  const unchanged = (value === original) || (!value && (!original || original === "—"));
  if (unchanged) { el.textContent = inlineFieldDisplay(el, original); return; }

  try {
    const resp = await _csrfFetch(`/api/return/${returnId}/field`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ field, value: value || null }),
    });
    const data = await resp.json();
    if (data.success) {
      const finalVal = field === "processor" && data.value != null ? data.value : value;
      if (field === "processor") {
        el.textContent   = (finalVal || "—");
        el.dataset.value = finalVal != null ? String(finalVal) : "";
      } else {
        el.textContent   = value || "—";
        el.dataset.value = value;
      }
      flash(el);
    } else {
      el.textContent = inlineFieldDisplay(el, original);
    }
  } catch {
    el.textContent = inlineFieldDisplay(el, original);
  }
}

async function toggleBool(el) {
  const returnId = el.dataset.returnId;
  const field    = el.dataset.field;
  const isTrue   = el.dataset.value === "1";
  const newVal   = isTrue ? 0 : 1;
  try {
    const resp = await _csrfFetch(`/api/return/${returnId}/field`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ field, value: newVal }),
    });
    const data = await resp.json();
    if (data.success) {
      el.dataset.value = String(newVal);
      el.innerHTML     = newVal
        ? `<span class="text-green-500 font-bold text-lg">✓</span>`
        : `<span class="text-slate-200 text-lg">○</span>`;
      flash(el);
    }
  } catch { /* ignore */ }
}

// ── Add note (return detail) ─────────────────────────────────────────────────

async function submitNote(returnId) {
  const textarea = document.getElementById("note-input");
  const text     = (textarea.value || "").trim();
  if (!text) return;

  try {
    const resp = await _csrfFetch(`/api/return/${returnId}/note`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ text }),
    });
    const data = await resp.json();

    if (data.success) {
      textarea.value = "";
      const list = document.getElementById("notes-list");
      const existing = list.querySelector(".text-center");
      if (existing) existing.remove();

      const item = document.createElement("div");
      item.className = "px-4 py-3 border-b border-slate-100";
      item.innerHTML = `
        <p class="text-sm text-slate-800 leading-snug">${escHtml(data.text)}</p>
        <p class="text-xs text-slate-400 mt-1">APP · just now</p>
      `;
      list.prepend(item);
      flash(item);
    } else if (data.error === "Duplicate note") {
      textarea.classList.add("ring-2", "ring-red-300");
      setTimeout(() => textarea.classList.remove("ring-2", "ring-red-300"), 1200);
    }
  } catch { /* ignore */ }
}

// ── Table quick-filter (client-side) ────────────────────────────────────────

let _tableFilterDebounce = 0;
let _tableFilterRaf      = 0;

function _cacheSearchLcase(row) {
  if (row._searchLcase === undefined) {
    row._searchLcase = (row.dataset.search || "").toLowerCase();
  }
  return row._searchLcase;
}

function runTableQuickFilter() {
  const input = document.getElementById("table-filter");
  if (!input) return;

  const q   = input.value.trim().toLowerCase();
  const all = document.querySelectorAll("tr[data-search]");
  let visible = 0;
  for (const row of all) {
    const lc    = _cacheSearchLcase(row);
    const match = !q || lc.includes(q);
    row.classList.toggle("tr-filter-hidden", !match);
    if (match) visible++;
  }
  const counter = document.getElementById("row-count");
  if (counter) counter.textContent = visible;
  syncDashboardTableSelection();
}

function scheduleTableQuickFilter(_immediate) {
  if (_tableFilterRaf) cancelAnimationFrame(_tableFilterRaf);
  _tableFilterRaf = requestAnimationFrame(() => {
    _tableFilterRaf = 0;
    runTableQuickFilter();
  });
}

function initTableFilter() {
  const input = document.getElementById("table-filter");
  if (!input) return;

  const onFilterInput = () => {
    if (_tableFilterDebounce) {
      clearTimeout(_tableFilterDebounce);
    }
    const shortQuery = (input.value || "").trim().length <= 1;
    const delay      = shortQuery ? 0 : 100;
    _tableFilterDebounce = setTimeout(() => {
      _tableFilterDebounce = 0;
      scheduleTableQuickFilter();
    }, delay);
  };

  input.addEventListener("input", onFilterInput, { passive: true });
}

// ── Dashboard row checkboxes (persists in sessionStorage across status/form/preparer) ─

function isDashboardRowVisible(tr) {
  if (!tr?.matches("tr[data-search]")) return false;
  if (tr.classList.contains("tr-filter-hidden")) return false;
  return true;
}

function getVisibleDataRows() {
  return Array.from(document.querySelectorAll(".data-table tbody tr[data-search]")).filter(
    isDashboardRowVisible
  );
}

function getSelectionStorageKey() {
  const y = document.body?.dataset?.year || new Date().getFullYear();
  return `taxops_selected_returns_${y}`;
}

function readPersistentSet() {
  try {
    const raw = sessionStorage.getItem(getSelectionStorageKey());
    if (!raw) return new Set();
    const ar = JSON.parse(raw);
    if (!Array.isArray(ar)) return new Set();
    return new Set(ar.map((x) => parseInt(x, 10)).filter((n) => !Number.isNaN(n)));
  } catch {
    return new Set();
  }
}

function writePersistentSet(s) {
  try {
    sessionStorage.setItem(getSelectionStorageKey(), JSON.stringify([...s].sort((a, b) => a - b)));
  } catch { /* private mode, quota, etc. */ }
}

function applyPersistentToDom() {
  const s = readPersistentSet();
  document.querySelectorAll(".row-select").forEach((cb) => {
    const id = parseInt(cb.dataset.returnId, 10);
    if (Number.isNaN(id)) return;
    const want = s.has(id);
    if (cb.checked !== want) cb.checked = want;
  });
}

function setAllVisibleCheckboxes(checked) {
  const s = readPersistentSet();
  getVisibleDataRows().forEach((tr) => {
    const cb = tr.querySelector(".row-select");
    if (!cb) return;
    const id = parseInt(cb.dataset.returnId, 10);
    if (Number.isNaN(id)) return;
    if (checked) s.add(id);
    else s.delete(id);
    cb.checked = checked;
  });
  writePersistentSet(s);
  syncDashboardTableSelection();
}

function clearAllRowCheckboxes() {
  try {
    sessionStorage.removeItem(getSelectionStorageKey());
  } catch { /* */ }
  document.querySelectorAll(".row-select").forEach((cb) => {
    cb.checked = false;
  });
  syncDashboardTableSelection();
}

function syncDashboardTableSelection() {
  const master = document.getElementById("table-select-all");
  if (!master) return;

  const s = readPersistentSet();
  const checkboxes = getVisibleDataRows()
    .map((tr) => tr.querySelector(".row-select"))
    .filter(Boolean);
  const nInSetOnVisible = checkboxes.filter((cb) => {
    const id = parseInt(cb.dataset.returnId, 10);
    return !Number.isNaN(id) && s.has(id);
  }).length;

  if (checkboxes.length === 0) {
    master.checked = false;
    master.indeterminate = false;
  } else {
    master.checked = nInSetOnVisible === checkboxes.length;
    master.indeterminate = nInSetOnVisible > 0 && nInSetOnVisible < checkboxes.length;
  }

  const sc = document.getElementById("selected-count");
  if (sc) sc.textContent = String(s.size);

  syncBulkActionsBar();
}

function syncBulkActionsBar() {
  const bar = document.getElementById("bulk-actions-bar");
  const bn = document.getElementById("bulk-actions-count");
  if (!bar || !bn) return;
  const n = readPersistentSet().size;
  bn.textContent = String(n);
  bar.style.display = n > 0 ? "flex" : "none";
}

/** Dashboard bulk overlays + toast(BULK-2…6) — no-ops unless toolbar markup exists. */
let _bulkConfirm = null;
let _bulkCommitInFlight = false;
let _dashboardBulkToastTimer = null;

function openBulkModal(el) {
  if (!el) return;
  el.classList.remove("hidden");
  el.classList.add("flex");
}

function closeBulkModal(el) {
  if (!el) return;
  el.classList.add("hidden");
  el.classList.remove("flex");
}

function hideDashboardBulkToast() {
  const wrap = document.getElementById("dashboard-bulk-toast");
  if (!wrap) return;
  wrap.classList.add("opacity-0", "translate-y-2");
  wrap.classList.remove("opacity-100", "translate-y-0");
}

/** @param {'success'|'error'|'warn'} variant */
function showDashboardBulkToast(variant, innerHtml, durationMs) {
  const wrap = document.getElementById("dashboard-bulk-toast");
  const inner = document.getElementById("dashboard-bulk-toast-inner");
  if (!wrap || !inner) return;

  clearTimeout(_dashboardBulkToastTimer);
  const skin =
    variant === "success"
      ? "border-green-200 bg-green-50 text-green-950"
      : variant === "warn"
        ? "border-amber-200 bg-amber-50 text-amber-950"
        : "border-red-200 bg-red-50 text-red-950";

  inner.className = `rounded-xl border px-4 py-3 shadow-2xl text-sm pointer-events-auto ${skin}`;
  inner.innerHTML = innerHtml;

  wrap.classList.remove("opacity-0", "translate-y-2");
  wrap.classList.add("opacity-100", "translate-y-0");

  const ms =
    typeof durationMs === "number"
      ? durationMs
      : variant === "success"
        ? 5500
        : 14000;

  _dashboardBulkToastTimer = setTimeout(() => hideDashboardBulkToast(), ms);
}

function formatBulkErrorList(errors) {
  if (!errors || errors.length === 0) return "";
  const cap = errors.slice(0, 35);
  const items = cap
    .map((e) => {
      const id = e.return_id != null ? `#${e.return_id}` : "—";
      return `<li class="leading-snug"><span class="font-mono">${escHtml(String(id))}</span> — ${escHtml(
        String(e.error || ""),
      )}</li>`;
    })
    .join("");
  const more =
    errors.length > cap.length
      ? `<li class="text-slate-600 list-none mt-1">…and ${errors.length - cap.length} more.</li>`
      : "";
  return `<ul class="list-disc pl-4 mt-2 space-y-0.5 text-xs">${items}${more}</ul>`;
}

async function dashboardBulkFetchJson(endpoint, payload) {
  let resp;
  try {
    resp = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    return { ok: false, status: 0, body: {}, networkError: String((e && e.message) || e || "network") };
  }

  const raw = await resp.text().catch(() => "");
  let body = {};
  if (raw) {
    try {
      body = JSON.parse(raw);
    } catch {
      body = { error: raw.slice(0, 200) };
    }
  }

  if (resp.status === 401) {
    window.location.href = `/login?next=${encodeURIComponent(
      `${window.location.pathname}${window.location.search}`,
    )}`;
    return { ok: false, status: 401, body, unauthorized: true };
  }

  return { ok: resp.ok, status: resp.status, body };
}

function initDashboardBulkActions() {
  const bar = document.getElementById("bulk-actions-bar");
  const statusModal = document.getElementById("bulk-status-modal");
  const prepModal = document.getElementById("bulk-preparer-modal");
  if (!bar || !statusModal || !prepModal) return;

  const stPick = document.getElementById("bulk-status-pick");
  const prPick = document.getElementById("bulk-preparer-pick");

  [statusModal, prepModal].forEach((modal) => {
    modal.querySelectorAll(".bulk-modal-cancel").forEach((b) => {
      b.addEventListener("click", () => {
        closeBulkModal(statusModal);
        closeBulkModal(prepModal);
        _bulkConfirm = null;
      });
    });
    modal.addEventListener("click", (ev) => {
      if (ev.target === modal) {
        closeBulkModal(modal);
        _bulkConfirm = null;
      }
    });
  });

  document.getElementById("btn-bulk-status-open")?.addEventListener("click", () => {
    const ids =
      typeof window.getSelectedReturnIds === "function" ? window.getSelectedReturnIds() : [];
    if (!ids.length) {
      showDashboardBulkToast(
        "warn",
        `<p class="font-semibold">No returns selected</p><p class="text-xs mt-1 opacity-90">Select one or more rows with the checkboxes first.</p>`,
        5000,
      );
      return;
    }
    const st = stPick?.value || "";
    const sm = document.getElementById("bulk-status-modal-summary");
    if (sm) {
      sm.innerHTML = `
        <p>Set client status on <strong class="tabular-nums">${ids.length}</strong> return${ids.length === 1 ? "" : "s"} 
        to <strong>${escHtml(st)}</strong>.</p>
        <p class="text-xs mt-2 text-slate-500">The server applies this as one transaction — if any return cannot move, nothing changes and you’ll see details below.</p>`;
    }
    _bulkConfirm = { kind: "status", ids, status: st };
    openBulkModal(statusModal);
  });

  document.getElementById("btn-bulk-preparer-open")?.addEventListener("click", () => {
    const ids =
      typeof window.getSelectedReturnIds === "function" ? window.getSelectedReturnIds() : [];
    if (!ids.length) {
      showDashboardBulkToast(
        "warn",
        `<p class="font-semibold">No returns selected</p><p class="text-xs mt-1 opacity-90">Select one or more rows with the checkboxes first.</p>`,
        5000,
      );
      return;
    }
    const raw = prPick?.value ?? "";
    const label =
      raw && String(raw).trim()
        ? escHtml(preparerListLabelJs(String(raw)))
        : '<span class="italic">clear assignment</span>';
    const sm = document.getElementById("bulk-preparer-modal-summary");
    if (sm) {
      sm.innerHTML = `
        <p>Assign preparer on <strong class="tabular-nums">${ids.length}</strong> return${ids.length === 1 ? "" : "s"} 
        to <strong>${label}</strong>.</p>
        <p class="text-xs mt-2 text-slate-500">Rows already set to this preparer count as skipped (shown in the success toast).</p>`;
    }
    _bulkConfirm = { kind: "preparer", ids, processor: raw.trim() === "" ? null : raw };
    openBulkModal(prepModal);
  });

  document.getElementById("bulk-status-modal-commit")?.addEventListener("click", async () => {
    if (_bulkCommitInFlight || !_bulkConfirm || _bulkConfirm.kind !== "status") return;
    const pending = _bulkConfirm;
    const btns = statusModal.querySelectorAll("button");
    _bulkCommitInFlight = true;
    btns.forEach((x) => {
      x.disabled = true;
    });
    try {
      const result = await dashboardBulkFetchJson("/api/returns/bulk-status", {
        return_ids: pending.ids,
        status: pending.status,
      });
      if (result.unauthorized) return;

      closeBulkModal(statusModal);
      const b = result.body || {};
      const errs = b.errors || [];

      if (!result.ok) {
        if (result.networkError) {
          showDashboardBulkToast(
            "error",
            `<p class="font-semibold">Bulk status failed</p><p class="text-xs mt-1">${escHtml(result.networkError)}</p>`,
          );
          return;
        }
        const msg = b.error
          ? `<p>${escHtml(String(b.error))}</p>`
          : `<p class="font-semibold">Could not bulk update status (${result.status})</p>`;
        showDashboardBulkToast("error", msg + formatBulkErrorList(errs));
        return;
      }

      const nUp = typeof b.changed === "number" ? b.changed : pending.ids.length;
      showDashboardBulkToast(
        "success",
        `<p class="font-semibold">Updated ${nUp} return${nUp !== 1 ? "s" : ""}</p><p class="text-xs mt-1">Reloading the dashboard…</p>`,
        4000,
      );
      window.setTimeout(() => window.location.reload(), 350);
    } finally {
      _bulkCommitInFlight = false;
      btns.forEach((x) => {
        x.disabled = false;
      });
      _bulkConfirm = null;
    }
  });

  document.getElementById("bulk-preparer-modal-commit")?.addEventListener("click", async () => {
    if (_bulkCommitInFlight || !_bulkConfirm || _bulkConfirm.kind !== "preparer") return;
    const pending = _bulkConfirm;
    const btns = prepModal.querySelectorAll("button");
    _bulkCommitInFlight = true;
    btns.forEach((x) => {
      x.disabled = true;
    });
    try {
      const result = await dashboardBulkFetchJson("/api/returns/bulk-processor", {
        return_ids: pending.ids,
        processor: pending.processor,
      });
      if (result.unauthorized) return;

      closeBulkModal(prepModal);
      const b = result.body || {};
      const errs = b.errors || [];

      if (!result.ok) {
        if (result.networkError) {
          showDashboardBulkToast(
            "error",
            `<p class="font-semibold">Bulk preparer failed</p><p class="text-xs mt-1">${escHtml(result.networkError)}</p>`,
          );
          return;
        }
        const msg = b.error
          ? `<p>${escHtml(String(b.error))}</p>`
          : `<p class="font-semibold">Could not bulk assign preparer (${result.status})</p>`;
        showDashboardBulkToast("error", msg + formatBulkErrorList(errs));
        return;
      }

      const ch = typeof b.changed === "number" ? b.changed : 0;
      const skipped = pending.ids.length - ch;

      if (ch > 0) {
        showDashboardBulkToast(
          "success",
          `<p class="font-semibold">Preparer updated on ${ch} return${ch !== 1 ? "s" : ""}</p>` +
            (skipped > 0 ? `<p class="text-xs mt-1 opacity-90">${skipped} already matched — skipped.</p>` : "") +
            `<p class="text-xs mt-1">Reloading the dashboard…</p>`,
          5500,
        );
        window.setTimeout(() => window.location.reload(), 380);
      } else {
        showDashboardBulkToast(
          "warn",
          `<p class="font-semibold">Nothing to update</p><p class="text-xs mt-1">Each selected row already had this preparer assignment.</p>`,
          7000,
        );
      }
    } finally {
      _bulkCommitInFlight = false;
      btns.forEach((x) => {
        x.disabled = false;
      });
      _bulkConfirm = null;
    }
  });
}

function initDashboardTableSelection() {
  if (!document.getElementById("table-select-all")) return;

  applyPersistentToDom();

  const master = document.getElementById("table-select-all");
  master.addEventListener("change", () => {
    setAllVisibleCheckboxes(!!master.checked);
  });

  document.getElementById("btn-select-all-visible")?.addEventListener("click", (e) => {
    e.preventDefault();
    setAllVisibleCheckboxes(true);
  });

  document.getElementById("btn-clear-row-selection")?.addEventListener("click", (e) => {
    e.preventDefault();
    clearAllRowCheckboxes();
  });

  document.querySelector(".data-table tbody")?.addEventListener("change", (e) => {
    if (e.target.classList?.contains("row-select")) {
      const id = parseInt(e.target.dataset.returnId, 10);
      if (Number.isNaN(id)) {
        syncDashboardTableSelection();
        return;
      }
      const s = readPersistentSet();
      if (e.target.checked) s.add(id);
      else s.delete(id);
      writePersistentSet(s);
      syncDashboardTableSelection();
    }
  });

  window.getSelectedReturnIds = () => [...readPersistentSet()].sort((a, b) => a - b);

  syncDashboardTableSelection();
}

// ── Year picker ──────────────────────────────────────────────────────────────

function initYearPicker() {
  const picker = document.getElementById("year-picker");
  if (!picker) return;
  picker.addEventListener("change", () => {
    const url = new URL(window.location);
    url.searchParams.set("year", picker.value);
    window.location = url.toString();
  });
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function flash(el) {
  el.classList.add("flash");
  el.addEventListener("animationend", () => el.classList.remove("flash"), { once: true });
}

function escHtml(str) {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Close status menus when clicking outside
document.addEventListener("click", e => {
  if (!e.target.closest(".status-dropdown")) {
    document.querySelectorAll(".status-menu").forEach(m => m.classList.add("hidden"));
  }
});

// ── PROD-6 Client error boundary (global handlers + optional server report) ─

const _CLIENT_ERR_DEDUP_MS = 60_000;
const _clientErrDedup = new Map();

const _CLIENT_ERR_DEFAULT_DETAIL =
  "A script hit an unexpected issue. Your data on the server is fine. Reload if buttons or searches stop responding. A short report was posted to the server log for staff.";

function _clientErrDedupKey(parts) {
  return parts.join("\u241e");
}

function _clientErrShouldSend(key) {
  const now = Date.now();
  const t = _clientErrDedup.get(key);
  if (t !== undefined && now - t < _CLIENT_ERR_DEDUP_MS) return false;
  _clientErrDedup.set(key, now);
  if (_clientErrDedup.size > 200) _clientErrDedup.clear();
  return true;
}

function _truncateClientErrStr(s, max) {
  const t = typeof s === "string" ? s : String(s);
  return t.length > max ? t.slice(0, max - 1) + "\u2026" : t;
}

function postClientErrorReport(payload) {
  try {
    const pk = _clientErrDedupKey([
      payload.kind || "",
      payload.message || "",
      payload.filename || "",
      String(payload.lineno ?? ""),
    ]);
    if (!_clientErrShouldSend(pk)) return;
    const body = {
      kind: _truncateClientErrStr(payload.kind || "unknown", 32),
      message: _truncateClientErrStr(payload.message || "", 2000),
      page_url: _truncateClientErrStr(payload.page_url || "", 2000),
      filename: _truncateClientErrStr(payload.filename || "", 500),
      lineno: payload.lineno,
      colno: payload.colno,
      stack: _truncateClientErrStr(payload.stack || "", 8000),
    };
    _csrfFetch("/api/client-error", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify(body),
    }).catch(() => {});
  } catch {
    /* never throw — error handlers must stay safe */
  }
}

function showClientErrorBoundaryFriendly() {
  const panel = document.getElementById("client-error-boundary");
  const detail = document.getElementById("client-error-boundary-detail");
  if (!panel || !detail) return;
  detail.textContent = _CLIENT_ERR_DEFAULT_DETAIL;
  panel.classList.remove("hidden");
}

(function registerTaxopsClientFatalHandlers() {
  window.addEventListener("error", (ev) => {
    const msg = ev.message ? String(ev.message) : "Script error";
    showClientErrorBoundaryFriendly();
    postClientErrorReport({
      kind: "error",
      message: msg,
      filename: ev.filename || "",
      lineno: typeof ev.lineno === "number" ? ev.lineno : null,
      colno: typeof ev.colno === "number" ? ev.colno : null,
      stack: ev.error && ev.error.stack ? String(ev.error.stack) : "",
      page_url: window.location.href || "",
    });
  });

  window.addEventListener("unhandledrejection", (ev) => {
    const r = ev.reason;
    let msg = "Unhandled promise rejection";
    let stack = "";
    if (typeof r === "string") msg = r;
    else if (r && typeof r === "object" && typeof r.message === "string") {
      msg = r.message || msg;
      if (typeof r.stack === "string") stack = r.stack;
    }
    showClientErrorBoundaryFriendly();
    postClientErrorReport({
      kind: "unhandledrejection",
      message: msg,
      filename: "",
      lineno: null,
      colno: null,
      stack,
      page_url: window.location.href || "",
    });
  });
})();

function wireClientErrorBoundaryButtons() {
  const panel = document.getElementById("client-error-boundary");
  const btnDismiss = document.getElementById("client-error-boundary-dismiss");
  const btnReload = document.getElementById("client-error-boundary-reload");
  if (!panel || !btnDismiss || !btnReload) return;
  btnDismiss.addEventListener("click", () => panel.classList.add("hidden"));
  btnReload.addEventListener("click", () => window.location.reload());
}

// ── Init ─────────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  wireClientErrorBoundaryButtons();
  initSearch();
  initInlineEdit();
  initTableFilter();
  initDashboardTableSelection();
  initDashboardBulkActions();
  initYearPicker();

  // TOUR-3: help icon resets server-side completion then restarts the tour
  const _tourHelpBtn = document.getElementById("tour-help-btn");
  if (_tourHelpBtn) {
    _tourHelpBtn.addEventListener("click", async () => {
      try {
        await _csrfFetch("/api/tour/reset", { method: "POST" });
      } catch (_) { /* non-fatal — still restart visually */ }
      if (window.TaxOpsTour) window.TaxOpsTour.restart();
    });
  }

  // Press "/" to focus search from anywhere
  document.addEventListener("keydown", e => {
    const tag = document.activeElement?.tagName;
    if (e.key === "/" && tag !== "INPUT" && tag !== "TEXTAREA" && tag !== "SELECT") {
      e.preventDefault();
      document.getElementById("global-search")?.focus();
    }
  });
});

// ── TOUR-1/2/3: Staff onboarding tooltip tour ─────────────────────────────────
// Vanilla JS — no external library. Exposes window.TaxOpsTour.start() and
// window.TaxOpsTour.restart(). State persisted in app_settings via API.

window.TaxOpsTour = (function () {

  // ── Step definitions (TOUR-2) ───────────────────────────────────────────────
  // Each step: { selector, title, body, position, page, navigate }
  //   page      — URL prefix the step lives on (null = any page)
  //   navigate  — URL to go to before this step (triggers page reload + resume)
  const STEPS = [
    {
      selector: "#global-search",
      title: "Find any client instantly",
      body: "Type a name or return number here. Results appear as you type. This is the fastest way to get to any client or return.",
      position: "below",
      page: "/",
    },
    {
      selector: "#status-pills",
      title: "Track where every return stands",
      body: "These tabs filter by workflow status. PROCESSING means actively being worked. PICKUP means ready for the client. Click any tab to see only those returns.",
      position: "below",
      page: "/",
    },
    {
      selector: "#documents",
      title: "Every document in one place",
      body: "W-2s, 1099s, and anything the client emails gets saved here automatically. You can also upload documents directly. Click any file to view it.",
      position: "below",
      page: "/return/",
      navigate: "_first_return",
    },
    {
      selector: "#return-status-control",
      title: "Change status as work progresses",
      body: "Click the status badge to change where this return stands — PROCESSING while you work it, FINALIZE when it needs review, PICKUP when the client can collect.",
      position: "below",
      page: "/return/",
    },
    {
      selector: "#notes-card",
      title: "Keep your team in sync",
      body: "Leave notes that are visible to everyone on the team. Useful for flagging missing documents, client callbacks, or anything the next person working this return needs to know.",
      position: "below",
      page: "/return/",
    },
    {
      selector: "#zone-a-section",
      title: "Incoming client documents",
      body: "When a client emails their documents they appear here. Review and confirm to attach them to the right return. The system matches clients automatically — you just verify.",
      position: "below",
      page: "/email-review",
      navigate: "/email-review",
    },
    {
      selector: "#site-header",
      title: "You are ready",
      body: "That covers the essentials. You can relaunch this tour anytime from the help icon in the top navigation. If you have questions check the runbook or ask your admin.",
      position: "below",
      page: null,
    },
  ];

  const SESSION_KEY = "taxops_tour_step";
  let _currentStep = 0;
  let _active = false;

  // ── DOM helpers ─────────────────────────────────────────────────────────────

  function _cleanup() {
    document.querySelectorAll("[data-taxops-tour]").forEach(el => el.remove());
    document.querySelectorAll("[data-taxops-tour-highlight]").forEach(el => {
      el.style.position = "";
      el.style.zIndex = "";
      el.style.outline = "";
      el.removeAttribute("data-taxops-tour-highlight");
    });
    _active = false;
  }

  function _backdrop() {
    const bd = document.createElement("div");
    bd.setAttribute("data-taxops-tour", "backdrop");
    Object.assign(bd.style, {
      position: "fixed",
      inset: "0",
      zIndex: "9998",
      pointerEvents: "all",
    });
    bd.addEventListener("click", () => dismiss());
    document.body.appendChild(bd);
    return bd;
  }

  function _highlightEl(el) {
    const prev = document.querySelector("[data-taxops-tour-highlight]");
    if (prev) {
      prev.style.position = "";
      prev.style.zIndex = "";
      prev.style.outline = "";
      prev.removeAttribute("data-taxops-tour-highlight");
    }
    el.setAttribute("data-taxops-tour-highlight", "1");
    const cs = window.getComputedStyle(el);
    if (cs.position === "static") el.style.position = "relative";
    el.style.zIndex = "9999";
    el.style.outline = "2px solid #6366f1";
    el.style.borderRadius = el.style.borderRadius || "6px";
  }

  function _tooltip(step, idx, total) {
    const tt = document.createElement("div");
    tt.setAttribute("data-taxops-tour", "tooltip");
    Object.assign(tt.style, {
      position: "fixed",
      zIndex: "10000",
      maxWidth: "320px",
      background: "#1e293b",
      color: "#f1f5f9",
      borderRadius: "12px",
      padding: "16px 18px",
      boxShadow: "0 8px 32px rgba(0,0,0,0.35)",
      opacity: "0",
      transition: "opacity 150ms ease",
      pointerEvents: "all",
    });

    const progress = `<span style="font-size:11px;color:#94a3b8;display:block;margin-bottom:8px;">Step ${idx + 1} of ${total}</span>`;
    const titleHtml = `<p style="font-weight:700;font-size:14px;margin:0 0 6px;">${escHtml(step.title)}</p>`;
    const bodyHtml  = `<p style="font-size:13px;line-height:1.55;margin:0 0 14px;color:#cbd5e1;">${escHtml(step.body)}</p>`;
    const isLast    = idx === total - 1;
    const nextLabel = isLast ? "Done ✓" : "Next →";
    const btnRow = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">
        <button data-taxops-tour="skip"
          style="font-size:12px;color:#94a3b8;background:none;border:none;cursor:pointer;padding:0;">
          Skip tour
        </button>
        <button data-taxops-tour="next"
          style="font-size:13px;font-weight:600;background:#6366f1;color:#fff;border:none;
                 border-radius:7px;padding:7px 16px;cursor:pointer;">
          ${nextLabel}
        </button>
      </div>`;

    tt.innerHTML = progress + titleHtml + bodyHtml + btnRow;
    document.body.appendChild(tt);
    requestAnimationFrame(() => { tt.style.opacity = "1"; });

    tt.querySelector("[data-taxops-tour='skip']").addEventListener("click", () => dismiss());
    tt.querySelector("[data-taxops-tour='next']").addEventListener("click", () => {
      if (isLast) complete();
      else goToStep(_currentStep + 1);
    });
    return tt;
  }

  function _positionTooltip(tt, target) {
    const tr = target.getBoundingClientRect();
    const ttH = tt.offsetHeight || 160;
    const ttW = tt.offsetWidth  || 320;
    const vp  = { w: window.innerWidth, h: window.innerHeight };
    const margin = 12;

    let top, left;
    if (tr.bottom + ttH + margin < vp.h) {
      top  = tr.bottom + margin;
      left = Math.min(Math.max(tr.left, margin), vp.w - ttW - margin);
    } else {
      top  = Math.max(tr.top - ttH - margin, margin);
      left = Math.min(Math.max(tr.left, margin), vp.w - ttW - margin);
    }
    tt.style.top  = top  + "px";
    tt.style.left = left + "px";
  }

  // ── Navigation helpers ──────────────────────────────────────────────────────

  function _firstReturnUrl() {
    const links = document.querySelectorAll("a[href^='/return/']");
    if (links.length) return links[0].getAttribute("href");
    const trs = document.querySelectorAll("tr[data-id]");
    if (trs.length) return `/return/${trs[0].dataset.id}`;
    return null;
  }

  function _navigateForStep(step, stepIdx) {
    if (!step.navigate) return false;
    const nav = step.navigate === "_first_return" ? _firstReturnUrl() : step.navigate;
    if (!nav) return false;
    const path = window.location.pathname;
    if (path.startsWith("/return/") && step.page === "/return/") return false;
    if (step.navigate !== "_first_return" && path.startsWith(step.navigate)) return false;
    sessionStorage.setItem(SESSION_KEY, String(stepIdx));
    window.location.href = nav;
    return true;
  }

  // ── Step runner ─────────────────────────────────────────────────────────────

  function goToStep(idx) {
    if (idx >= STEPS.length) { complete(); return; }
    _currentStep = idx;

    const step = STEPS[idx];

    // Navigate to a different page if needed
    if (_navigateForStep(step, idx)) return;

    // Remove previous tooltip
    document.querySelectorAll("[data-taxops-tour='tooltip']").forEach(el => el.remove());

    const target = document.querySelector(step.selector);
    if (!target) {
      // Target not found on this page — skip silently
      goToStep(idx + 1);
      return;
    }

    _highlightEl(target);
    target.scrollIntoView({ behavior: "smooth", block: "center" });

    setTimeout(() => {
      const tt = _tooltip(step, idx, STEPS.length);
      setTimeout(() => _positionTooltip(tt, target), 20);
      // Reposition on scroll/resize
      const repos = () => _positionTooltip(tt, target);
      window.addEventListener("scroll", repos, { passive: true });
      window.addEventListener("resize", repos, { passive: true });
      tt._cleanupRepos = () => {
        window.removeEventListener("scroll", repos);
        window.removeEventListener("resize", repos);
      };
    }, 80);
  }

  // ── Public API ───────────────────────────────────────────────────────────────

  function start() {
    if (_active) return;
    _active = true;
    _cleanup();
    _backdrop();
    goToStep(0);
  }

  function restart() {
    _cleanup();
    _active = true;
    sessionStorage.removeItem(SESSION_KEY);
    _backdrop();
    goToStep(0);
  }

  function dismiss() {
    sessionStorage.removeItem(SESSION_KEY);
    _cleanup();
  }

  function complete() {
    sessionStorage.removeItem(SESSION_KEY);
    _cleanup();
    _csrfFetch("/api/tour/complete", { method: "POST" }).catch(() => {});
  }

  // ── Resume from sessionStorage after page navigation ────────────────────────

  function _maybeResume() {
    const pending = sessionStorage.getItem(SESSION_KEY);
    if (pending === null) return;
    const idx = parseInt(pending, 10);
    if (isNaN(idx) || idx < 0 || idx >= STEPS.length) {
      sessionStorage.removeItem(SESSION_KEY);
      return;
    }
    sessionStorage.removeItem(SESSION_KEY);
    setTimeout(() => {
      _active = true;
      _backdrop();
      goToStep(idx);
    }, 500);
  }

  document.addEventListener("DOMContentLoaded", _maybeResume);

  return { start, restart, dismiss, complete };

})();
