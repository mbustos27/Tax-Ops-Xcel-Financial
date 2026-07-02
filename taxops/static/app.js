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

// ── I18N-4: JS translation helper ────────────────────────────────────────────
// Populated on DOMContentLoaded from GET /api/translations (current session locale).
window._t = {};
function t(key) {
  return window._t[key] !== undefined ? window._t[key] : key;
}
(function _loadTranslations() {
  fetch("/api/translations")
    .then(function (r) { return r.ok ? r.json() : {}; })
    .then(function (data) { window._t = data || {}; })
    .catch(function () { /* fail silently — English fallback via key */ });
})();

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
    container.innerHTML = `<div class="px-4 py-3 text-sm text-slate-400">${t("no_results")}</div>`;
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

function _closeAllStatusMenus() {
  document.querySelectorAll(".status-menu").forEach(m => {
    m.classList.add("hidden");
    m.style.cssText = "";
    // Restore menu to its original DOM position if it was teleported to <body>
    if (m._placeholder) {
      try { m._placeholder.replaceWith(m); } catch (_) { /* placeholder detached — leave menu hidden */ }
      m._placeholder = null;
    }
    if (m._triggerBtn) {
      m._triggerBtn._statusMenu = null;
      m._triggerBtn.setAttribute("aria-expanded", "false");
      m._triggerBtn = null;
    }
  });
}

function toggleStatusMenu(btn) {
  // The menu may already be teleported to <body>, making nextElementSibling a
  // comment node (null as an element). Use the stored ref when available.
  const menu = btn._statusMenu || btn.nextElementSibling;
  if (!menu) return;
  const hidden = menu.classList.contains("hidden");
  _closeAllStatusMenus();
  if (!hidden) return; // was open — just closed it above

  const rect = btn.getBoundingClientRect();

  // Teleport menu to <body> so no ancestor overflow/stacking-context clips it.
  // A comment node acts as a breadcrumb so we can restore it on close.
  const placeholder = document.createComment("status-menu-slot");
  menu.replaceWith(placeholder);
  menu._placeholder = placeholder;
  menu._triggerBtn  = btn;
  menu._badgeEl     = btn; // used by setStatus after teleport
  btn._statusMenu   = menu; // back-reference for re-entry when already teleported
  document.body.appendChild(menu);

  menu.style.cssText = `position:fixed;z-index:9999;top:${rect.bottom + 4}px;left:${rect.left}px;` +
    `background:#fff;border:1px solid #e2e8f0;border-radius:0.75rem;` +
    `box-shadow:0 10px 25px rgba(0,0,0,.12);overflow:hidden;`;
  menu.classList.remove("hidden");
  btn.setAttribute("aria-expanded", "true");

  // Reposition if the menu clips the right or bottom edge of the viewport
  requestAnimationFrame(() => {
    const mr = menu.getBoundingClientRect();
    if (mr.right > window.innerWidth - 8) {
      menu.style.left = Math.max(8, rect.right - mr.width) + "px";
    }
    if (mr.bottom > window.innerHeight - 8) {
      menu.style.top = Math.max(8, rect.top - mr.height - 4) + "px";
    }
  });

  const closeOnEscape = (e) => {
    if (e.key === "Escape") {
      _closeAllStatusMenus();
      btn.focus();
      document.removeEventListener("keydown", closeOnEscape);
    }
  };
  document.addEventListener("keydown", closeOnEscape);

  // Close when the scroll container scrolls (menu would drift otherwise)
  const wrap = document.getElementById("dashboard-table-wrap");
  if (wrap) wrap.addEventListener("scroll", _closeAllStatusMenus, { once: true });
}

async function setStatus(returnId, status, btn) {
  const menu  = btn.closest(".status-menu");
  const badge = menu._badgeEl || menu.previousElementSibling;
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

    _closeAllStatusMenus();
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

// Close status menus when clicking outside.
// Also guard against clicks inside a teleported .status-menu that is no
// longer a descendant of .status-dropdown (it lives at <body> level).
document.addEventListener("click", e => {
  if (!e.target.closest(".status-dropdown") && !e.target.closest(".status-menu")) {
    _closeAllStatusMenus();
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

// ── INTAKE-6: Phone auto-formatter ───────────────────────────────────────────
// Attach to any input with data-phone-input attribute.

function _formatPhoneValue(value) {
  const digits = value.replace(/\D/g, "").slice(0, 10);
  if (digits.length === 0) return "";
  if (digits.length <= 3) return "(" + digits;
  if (digits.length <= 6) return "(" + digits.slice(0, 3) + ") " + digits.slice(3);
  return "(" + digits.slice(0, 3) + ") " + digits.slice(3, 6) + "-" + digits.slice(6);
}

function initPhoneFormat() {
  function _attachPhone(el) {
    el.addEventListener("input", () => {
      const cur = el.selectionStart;
      const oldLen = el.value.length;
      el.value = _formatPhoneValue(el.value);
      const diff = el.value.length - oldLen;
      const next = Math.max(0, cur + diff);
      el.setSelectionRange(next, next);
    });
    el.addEventListener("paste", () => {
      setTimeout(() => { el.value = _formatPhoneValue(el.value); }, 0);
    });
  }
  document.querySelectorAll("[data-phone-input]").forEach(_attachPhone);
}

// ── INTAKE-1: Year expansion ──────────────────────────────────────────────────
// Attach to any input with data-year-input attribute.
// On blur: 2-digit 00-29 → 2000-2029, 30-99 → 1930-1999. 4-digit unchanged.

function initYearExpand() {
  function _expandYear(el) {
    el.addEventListener("blur", () => {
      const v = el.value.trim();
      if (!/^\d{2}$/.test(v)) return;
      const n = parseInt(v, 10);
      el.value = n <= 29 ? String(2000 + n) : String(1900 + n);
    });
  }
  document.querySelectorAll("[data-year-input]").forEach(_expandYear);
}

// ── INTAKE-2: SSN formatter ───────────────────────────────────────────────────
// Attach to any input with data-ssn-input attribute.
// Formats as XXX-XX-XXXX as user types. Uses type=password for shoulder safety.

function initSsnFormat() {
  function _formatSsn(value) {
    const d = value.replace(/\D/g, "").slice(0, 9);
    if (d.length <= 3) return d;
    if (d.length <= 5) return d.slice(0, 3) + "-" + d.slice(3);
    return d.slice(0, 3) + "-" + d.slice(3, 5) + "-" + d.slice(5);
  }
  function _attachSsn(el) {
    el.addEventListener("input", () => {
      const pos = el.selectionStart;
      const oldLen = el.value.length;
      el.value = _formatSsn(el.value);
      const diff = el.value.length - oldLen;
      const next = Math.max(0, pos + diff);
      el.setSelectionRange(next, next);
    });
    el.addEventListener("paste", () => {
      setTimeout(() => { el.value = _formatSsn(el.value); }, 0);
    });
  }
  document.querySelectorAll("[data-ssn-input]").forEach(_attachSsn);
}

// ── INTAKE-4: Spouse last name auto-fill ─────────────────────────────────────

function initSpouseAutoFill() {
  const taxpayerLast = document.getElementById("field-last_name");
  const spouseLast   = document.getElementById("field-spouse_last_name");
  const hint         = document.getElementById("spouse-autofill-hint");
  if (!taxpayerLast || !spouseLast) return;

  taxpayerLast.addEventListener("keyup", () => {
    if (spouseLast.value !== "" && !spouseLast.dataset.autofilled) return;
    spouseLast.value = taxpayerLast.value;
    spouseLast.dataset.autofilled = "true";
    if (hint) hint.classList.remove("hidden");
  });

  spouseLast.addEventListener("input", () => {
    delete spouseLast.dataset.autofilled;
    if (hint) hint.classList.add("hidden");
  });
}

// ── INTAKE-5: Address autocomplete (Nominatim / OpenStreetMap) ───────────────
// Debounced — fires after 400ms of no typing. PII-free query (address only).
// Fails silently if Nominatim is unreachable — never blocks intake submission.

function initAddressAutocomplete() {
  const field = document.getElementById("field-address");
  if (!field) return;

  const wrapper = field.parentElement;
  const prevPos = window.getComputedStyle(wrapper).position;
  if (prevPos === "static") wrapper.style.position = "relative";

  const dropdown = document.createElement("div");
  dropdown.id = "address-dropdown";
  dropdown.className = "hidden absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-2xl border border-slate-200 z-50 overflow-hidden max-h-56 overflow-y-auto";
  wrapper.appendChild(dropdown);

  let _addrTimer = null;

  field.addEventListener("input", () => {
    clearTimeout(_addrTimer);
    const q = field.value.trim();
    if (q.length < 5) { dropdown.classList.add("hidden"); return; }
    _addrTimer = setTimeout(() => _fetchAddresses(q), 400);
  });

  field.addEventListener("keydown", (e) => {
    if (e.key === "Escape") dropdown.classList.add("hidden");
  });

  document.addEventListener("click", (e) => {
    if (!field.contains(e.target) && !dropdown.contains(e.target))
      dropdown.classList.add("hidden");
  });

  async function _fetchAddresses(q) {
    try {
      const url =
        "https://nominatim.openstreetmap.org/search?q=" +
        encodeURIComponent(q) +
        "&countrycodes=us&format=json&addressdetails=1&limit=5";
      const resp = await fetch(url, { headers: { "User-Agent": "TaxOps/1.0" } });
      if (!resp.ok) { dropdown.classList.add("hidden"); return; }
      const results = await resp.json();
      _renderAddressResults(results);
    } catch {
      dropdown.classList.add("hidden");
    }
  }

  function _renderAddressResults(results) {
    if (!results || !results.length) { dropdown.classList.add("hidden"); return; }
    dropdown.innerHTML = results.map((r, i) =>
      `<button type="button" data-idx="${i}"
               class="addr-pick w-full text-left px-4 py-2.5 hover:bg-slate-50
                      transition-colors border-b border-slate-100 last:border-0 text-sm text-slate-800">
         ${escHtml(r.display_name || "")}
       </button>`
    ).join("");
    dropdown.classList.remove("hidden");
    dropdown.querySelectorAll(".addr-pick").forEach((btn) => {
      const idx = parseInt(btn.dataset.idx, 10);
      btn.addEventListener("click", () => _selectAddress(results[idx]));
    });
  }

  function _selectAddress(result) {
    const a = result.address || {};
    const street = [a.house_number, a.road].filter(Boolean).join(" ");
    const city   = a.city || a.town || a.village || a.hamlet || "";
    const state  = a.state || "";
    const zip    = a.postcode || "";
    field.value = [street, city, state, zip].filter(Boolean).join(", ");
    dropdown.classList.add("hidden");
  }
}

// ── BANK-1: Routing number → bank name auto-fill ──────────────────────────────
// Attach to any input with [data-routing-input]. On blur, if value is exactly
// 9 digits, calls GET /api/routing-number/{value}. On success fills the bank
// name field identified by [data-bank-name-target] and shows a small hint.
// On not-found does nothing — staff types manually.
// Routing numbers are never logged by the server route.

function initRoutingLookup() {
  document.querySelectorAll("[data-routing-input]").forEach((input) => {
    const targetId  = input.dataset.bankNameTarget;
    const hintEl    = document.getElementById("routing-lookup-hint");

    input.addEventListener("blur", async () => {
      const val = input.value.trim();
      if (!/^\d{9}$/.test(val)) return;
      try {
        const res  = await _csrfFetch(`/api/routing-number/${encodeURIComponent(val)}`);
        const data = await res.json();
        if (!data.found) return;
        const bankField = targetId ? document.getElementById(targetId) : null;
        if (bankField && !bankField.value) {
          bankField.value = data.bank_name;
        }
        if (hintEl) {
          hintEl.textContent = `${data.bank_name} — confirm or edit`;
          hintEl.classList.remove("hidden");
        }
        if (bankField) {
          bankField.addEventListener("input", () => {
            if (hintEl) hintEl.classList.add("hidden");
          }, { once: true });
        }
      } catch {
        // fail silently — staff types manually
      }
    });
  });
}

// ── Section 5: Inline field validation (blur-based, WCAG 3.3.1) ──────────────

function _fieldError(el, msg) {
  let err = document.getElementById("err-" + el.id);
  if (!err) {
    err = document.createElement("p");
    err.id = "err-" + el.id;
    err.className = "field-error";
    err.setAttribute("role", "alert");
    el.parentNode.appendChild(err);
    el.setAttribute("aria-describedby", "err-" + el.id);
  }
  if (msg) {
    err.textContent = msg;
    err.classList.add("visible");
    el.setAttribute("aria-invalid", "true");
  } else {
    err.classList.remove("visible");
    el.removeAttribute("aria-invalid");
  }
}

function initIntakeValidation() {
  // Phone fields: require 10 digits on blur
  document.querySelectorAll("[data-phone-input]").forEach((el) => {
    el.addEventListener("blur", () => {
      const digits = el.value.replace(/\D/g, "");
      if (el.value.length > 0 && digits.length < 10) {
        _fieldError(el, t("Invalid phone number — must be 10 digits"));
      } else {
        _fieldError(el, "");
      }
    });
    el.addEventListener("input", () => _fieldError(el, ""));
  });

  // Routing number: must be exactly 9 digits
  document.querySelectorAll("[data-routing-input]").forEach((el) => {
    el.addEventListener("blur", () => {
      if (el.value.length > 0 && !/^\d{9}$/.test(el.value)) {
        _fieldError(el, t("Routing numbers are 9 digits"));
      } else {
        _fieldError(el, "");
      }
    });
    el.addEventListener("input", () => _fieldError(el, ""));
  });

  // Year fields: expanded value should be 4-digit year in reasonable range
  document.querySelectorAll("[data-year-input]").forEach((el) => {
    el.addEventListener("blur", () => {
      const v = el.value.trim();
      if (!v) return;
      const n = parseInt(v, 10);
      if (isNaN(n) || v.length < 2 || n < 1900 || n > 2099) {
        _fieldError(el, t("Please enter a valid year"));
      } else {
        _fieldError(el, "");
      }
    });
    el.addEventListener("input", () => _fieldError(el, ""));
  });

  // Required fields: show message on blur if empty
  const intakeForm = document.getElementById("intake-form");
  if (intakeForm) {
    intakeForm.querySelectorAll("[required]").forEach((el) => {
      el.addEventListener("blur", () => {
        if (!el.value.trim()) {
          _fieldError(el, t("This field is required"));
        } else {
          _fieldError(el, "");
        }
      });
      el.addEventListener("input", () => _fieldError(el, ""));
    });
  }
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
  initPhoneFormat();
  initYearExpand();
  initSsnFormat();
  initSpouseAutoFill();
  initAddressAutocomplete();
  initRoutingLookup();
  initIntakeValidation();

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
//
// Step 0   — welcome modal (centered, no target)
// Steps 1–8 — tooltip anchored to target element
// skip/close/Esc all call complete() so the tour does not re-nag

window.TaxOpsTour = (function () {

  // ── Step definitions ────────────────────────────────────────────────────────
  // modal:true  → full-screen welcome card, no anchor
  // navigate    → "_first_return" | "/path" — navigate before showing this step
  //
  // Steps 0–16 (17 total; step 0 is the modal, steps 1–16 are tooltips).
  // All features shown are accessible to every role (receptionist, preparer,
  // admin) unless otherwise noted in a comment.
  function _steps() {
    return [
      /* 0 — welcome modal ─────────────────────────────────────────────── */
      {
        modal: true,
        title: t("tour_modal_title"),
        subtitle: t("tour_modal_subtitle"),
        bullets: [
          t("tour_modal_b1"),
          t("tour_modal_b2"),
          t("tour_modal_b3"),
          t("tour_modal_b4"),
        ],
      },

      /* 1 — example workflow modal — NEW */
      {
        modal: true,
        title: t("tour_wf_title"),
        subtitle: t("tour_wf_subtitle"),
        workflow: [
          { label: t("tour_wf_s1_label"), text: t("tour_wf_s1_body") },
          { label: t("tour_wf_s2_label"), text: t("tour_wf_s2_body") },
          { label: t("tour_wf_s3_label"), text: t("tour_wf_s3_body") },
          { label: t("tour_wf_s4_label"), text: t("tour_wf_s4_body") },
          { label: t("tour_wf_s5_label"), text: t("tour_wf_s5_body") },
          { label: t("tour_wf_s6_label"), text: t("tour_wf_s6_body") },
        ],
      },

      /* ── Dashboard ──────────────────────────────────────────────────── */

      /* 2 — global search (was 1) */
      { selector: "#global-search",
        title: t("tour_s1_title"),
        body:  t("tour_s1_body") },

      /* 3 — pipeline status tabs (was 2) */
      { selector: "#status-pills",
        title: t("tour_s2_title"),
        body:  t("tour_s2_body"),
        navigate: "/" },

      /* 4 — rejection alert bell (was 3) */
      { selector: "#reject-bell",
        title: t("tour_s_reject_bell_title"),
        body:  t("tour_s_reject_bell_body"),
        navigate: "/" },

      /* 5 — alert filter pills (was 4) */
      { selector: "#alert-filter-pills",
        title: t("tour_s_alert_pills_title"),
        body:  t("tour_s_alert_pills_body"),
        navigate: "/" },

      /* ── Intake ─────────────────────────────────────────────────────── */

      /* 6 — new intake nav link (was 5) */
      { selector: "#nav-new-intake",
        title: t("tour_s_intake_title"),
        body:  t("tour_s_intake_body"),
        navigate: "/" },

      /* 7 — returning client / reintake lookup (was 6) */
      { selector: "#client-lookup",
        title: t("tour_s_reintake_title"),
        body:  t("tour_s_reintake_body"),
        navigate: "/intake" },

      /* ── Client profile ─────────────────────────────────────────────── */

      /* 7 — client profile link on return detail */
      { selector: "#client-profile-link",
        title: t("tour_s_client_link_title"),
        body:  t("tour_s_client_link_body"),
        navigate: "_first_return" },

      /* 8 — contact / edit card on client profile */
      { selector: "#client-edit-section",
        title: t("tour_s_client_edit_title"),
        body:  t("tour_s_client_edit_body"),
        navigate: "_first_client" },

      /* 9 — multi-year comparison */
      { selector: "#multi-year-section",
        title: t("tour_s_client_multi_title"),
        body:  t("tour_s_client_multi_body") },

      /* ── Return detail ──────────────────────────────────────────────── */

      /* 10 — documents panel */
      { selector: "#documents",
        title: t("tour_s3_title"),
        body:  t("tour_s3_body"),
        navigate: "_first_return" },

      /* 11 — document upload */
      { selector: "#doc-upload-btn",
        title: t("tour_s_upload_title"),
        body:  t("tour_s_upload_body") },

      /* 12 — missing documents tracker */
      { selector: "#missing-docs-card",
        title: t("tour_s_missing_title"),
        body:  t("tour_s_missing_body") },

      /* 13 — status control */
      { selector: "#return-status-control",
        title: t("tour_s4_title"),
        body:  t("tour_s4_body") },

      /* 14 — team notes */
      { selector: "#notes-card",
        title: t("tour_s5_title"),
        body:  t("tour_s5_body") },

      /* 15 — return action sidebar — NEW */
      { selector: "#return-action-nav",
        title: t("tour_s_action_nav_title"),
        body:  t("tour_s_action_nav_body") },

      /* ── Dashboard — power features ─────────────────────────────────── */

      /* 16 — bulk actions */
      { selector: "#table-select-all",
        title: t("tour_s_bulk_title"),
        body:  t("tour_s_bulk_body"),
        navigate: "/" },

      /* 17 — saved filters / custom views */
      { selector: "#btn-save-dashboard-filter",
        title: t("tour_s_filters_title"),
        body:  t("tour_s_filters_body"),
        navigate: "/" },

      /* 18 — export to Excel — NEW */
      { selector: "#export-btn",
        title: t("tour_s_export_title"),
        body:  t("tour_s_export_body"),
        navigate: "/" },

      /* ── Queues ─────────────────────────────────────────────────────── */

      /* 19 — pickup queue */
      { selector: "#pickup-queue-wrap",
        title: t("tour_s_pickup_title"),
        body:  t("tour_s_pickup_body"),
        navigate: "/logout-queue" },

      /* 20 — e-file queue */
      { selector: "#batch-form",
        title: t("tour_s_efile_title"),
        body:  t("tour_s_efile_body"),
        navigate: "/efile-queue" },

      /* ── Accounting ─────────────────────────────────────────────────── */

      /* 21 — receipt tracking */
      { selector: "#receipt-queue-header",
        title: t("tour_s_receipts_title"),
        body:  t("tour_s_receipts_body"),
        navigate: "/accounting/receipts" },

      /* ── Global UI ──────────────────────────────────────────────────── */

      /* 22 — privacy mode */
      { selector: "#privacy-toggle",
        title: t("tour_s_privacy_title"),
        body:  t("tour_s_privacy_body") },

      /* 23 — season / year picker — NEW */
      { selector: "#year-picker",
        title: t("tour_s_year_picker_title"),
        body:  t("tour_s_year_picker_body") },

      /* 24 — wrap-up */
      { selector: "#tour-help-btn",
        title: t("tour_s7_title"),
        body:  t("tour_s7_body") },
    ];
  }

  const SESSION_KEY      = "taxops_tour_step";
  const PRESENT_KEY      = "taxops_tour_present";
  const PRESENT_STEP_MS  = 10000;   // ms per tooltip step in presentation mode
  const PRESENT_MODAL_MS = 14000;   // ms for modal steps (more content to read)
  let _currentStep  = 0;
  let _active       = false;
  let _reposOff     = null;   // cleanup fn for scroll/resize listeners
  let _presentMode   = false;
  let _presentPaused = false;
  let _presentTimer  = null;

  // ── Session helpers ───────────────────────────────────────────────────────────

  function _setResumeStep(idx) {
    sessionStorage.setItem(SESSION_KEY, String(idx));
    if (_presentMode) sessionStorage.setItem(PRESENT_KEY, "1");
  }

  // ── Cleanup ─────────────────────────────────────────────────────────────────

  function _cleanup() {
    _stopAutoAdvance();
    if (_reposOff) { _reposOff(); _reposOff = null; }
    document.querySelectorAll("[data-taxops-tour]").forEach(el => el.remove());
    document.querySelectorAll("[data-taxops-tour-hl]").forEach(el => {
      el.style.cssText = el._tourSaved || "";
      delete el._tourSaved;
      el.removeAttribute("data-taxops-tour-hl");
    });
    _active = false;
  }

  // ── Presentation auto-advance ────────────────────────────────────────────────

  function _stopAutoAdvance() {
    if (_presentTimer) { clearTimeout(_presentTimer); _presentTimer = null; }
  }

  function _startAutoAdvance(idx, ms) {
    _stopAutoAdvance();
    if (!_presentMode) return;
    _presentTimer = setTimeout(() => {
      if (!_presentMode || _presentPaused) return;
      _presentTimer = null;
      const steps = _steps();
      if (idx >= steps.length - 1) complete();
      else goToStep(idx + 1);
    }, ms);
  }

  function _togglePause() {
    _presentPaused = !_presentPaused;
    const btn = document.querySelector("[data-taxops-tour='pause-play']");
    if (btn) btn.textContent = _presentPaused ? "▶" : "⏸";
    const bar = document.querySelector("[data-taxops-tour='countdown']");
    if (_presentPaused) {
      _stopAutoAdvance();
      if (bar) bar.style.animationPlayState = "paused";
    } else {
      if (bar) {
        // Restart the countdown animation from full
        bar.style.animation = "none";
        bar.offsetHeight; // force reflow
        bar.style.animation = `tourCountdown ${PRESENT_STEP_MS / 1000}s linear forwards`;
        bar.style.animationPlayState = "running";
      }
      _startAutoAdvance(_currentStep, PRESENT_STEP_MS);
    }
  }

  // ── Backdrop (dimmed overlay) ────────────────────────────────────────────────

  function _backdrop() {
    const bd = document.createElement("div");
    bd.setAttribute("data-taxops-tour", "backdrop");
    Object.assign(bd.style, {
      position: "fixed", inset: "0",
      zIndex: "9000",
      background: "rgba(0,0,0,0)",
      transition: "background 250ms ease",
      pointerEvents: "none",
    });
    document.body.appendChild(bd);
    requestAnimationFrame(() => { bd.style.background = "rgba(0,0,0,0.48)"; });
    return bd;
  }

  // ── Spotlight highlight ──────────────────────────────────────────────────────

  function _highlight(el) {
    const prev = document.querySelector("[data-taxops-tour-hl]");
    if (prev) {
      prev.style.cssText = prev._tourSaved || "";
      delete prev._tourSaved;
      prev.removeAttribute("data-taxops-tour-hl");
    }
    if (!el) return;
    el._tourSaved = el.style.cssText;
    el.setAttribute("data-taxops-tour-hl", "1");
    const cs  = getComputedStyle(el);
    const pos = cs.position === "static" ? "relative" : cs.position;
    const br  = cs.borderRadius || "6px";
    // Use cssText append so existing inline styles are preserved
    el.style.cssText += `;position:${pos};z-index:9002;border-radius:${br};` +
      `box-shadow:0 0 0 3px var(--color-primary,#c08040),` +
                 `0 0 0 7px rgba(192,128,64,0.18),` +
                 `0 0 0 9999px rgba(0,0,0,0.48);` +
      `transition:box-shadow 200ms ease;`;
    el.scrollIntoView({ behavior: "smooth", block: "center" });
  }

  // ── Welcome modal (step 0) ───────────────────────────────────────────────────

  // _showWelcomeModal handles two modal variants:
  //   • step.bullets  → welcome card with checkmark list  (step 0)
  //   • step.workflow → numbered flow diagram card         (step 1+)
  // onBack is provided for steps > 0; null for the opening welcome.
  function _showWelcomeModal(step, onNext, onSkip, onBack) {
    let body;

    if (step.workflow) {
      // Numbered workflow flow — each item has { label, text }
      const items = step.workflow.map((item, i) => {
        const isLast = i === step.workflow.length - 1;
        return (
          `<li style="display:flex;gap:.75rem;align-items:flex-start;">` +
            `<div style="display:flex;flex-direction:column;align-items:center;flex-shrink:0;">` +
              `<div style="width:1.75rem;height:1.75rem;border-radius:50%;` +
                          `background:var(--color-primary,#c08040);color:#fff;` +
                          `display:flex;align-items:center;justify-content:center;` +
                          `font-size:.7rem;font-weight:700;">${i + 1}</div>` +
              (isLast ? "" :
                `<div style="width:2px;flex:1;min-height:.75rem;margin:.2rem 0;` +
                            `background:var(--border-default,#ddd);"></div>`) +
            `</div>` +
            `<div style="padding-bottom:${isLast ? "0" : ".75rem"};">` +
              `<div style="font-size:.8rem;font-weight:700;color:var(--text-primary);">${escHtml(item.label)}</div>` +
              `<div style="font-size:.8rem;color:var(--text-muted);margin-top:.15rem;">${escHtml(item.text)}</div>` +
            `</div>` +
          `</li>`
        );
      }).join("");

      body =
        `<ul style="list-style:none;padding:0;margin:0 0 1.5rem;display:flex;flex-direction:column;gap:0;">` +
          items +
        `</ul>` +
        `<div style="display:flex;justify-content:space-between;align-items:center;">` +
          `<button data-taxops-tour="modal-back" style="font-size:.8rem;color:var(--text-muted);background:none;border:1px solid var(--border-default,#ddd);border-radius:.4rem;padding:.35rem .75rem;cursor:pointer;">&larr; ${escHtml(t("tour_back") || "Back")}</button>` +
          `<button data-taxops-tour="modal-start" style="font-size:.875rem;font-weight:600;background:var(--color-primary,#c08040);color:#fff;border:none;border-radius:.5rem;padding:.5rem 1.25rem;cursor:pointer;">${escHtml(t("tour_next") || "Next")} &rarr;</button>` +
        `</div>`;
    } else {
      // Classic bullet welcome card
      const bullets = (step.bullets || []).map(b =>
        `<li style="display:flex;gap:.5rem;align-items:flex-start;font-size:.875rem;color:var(--text-secondary);">` +
          `<span style="color:var(--color-primary,#c08040);flex-shrink:0;margin-top:1px;">&#10003;</span>` +
          `<span>${escHtml(b)}</span>` +
        `</li>`
      ).join("");

      body =
        `<ul style="list-style:none;padding:0;margin:0 0 1.5rem;display:flex;flex-direction:column;gap:.5rem;">` +
          bullets +
        `</ul>` +
        `<div style="display:flex;justify-content:space-between;align-items:center;gap:.5rem;flex-wrap:wrap;">` +
          `<button data-taxops-tour="modal-skip" style="font-size:12px;color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0;">${escHtml(t("tour_skip") || "Skip tour")}</button>` +
          `<div style="display:flex;gap:.5rem;align-items:center;">` +
            `<button data-taxops-tour="modal-present" ` +
                    `title="Auto-advance with larger text — great for projectors" ` +
                    `style="font-size:.8rem;color:var(--color-primary,#c08040);background:none;` +
                           `border:1px solid var(--color-primary,#c08040);border-radius:.5rem;` +
                           `padding:.4rem .9rem;cursor:pointer;">&#128247; ${escHtml(t("tour_present") || "Present")}</button>` +
            `<button data-taxops-tour="modal-start" style="font-size:.875rem;font-weight:600;background:var(--color-primary,#c08040);color:#fff;border:none;border-radius:.5rem;padding:.5rem 1.25rem;cursor:pointer;">${escHtml(t("tour_begin") || "Start tour")} &rarr;</button>` +
          `</div>` +
        `</div>`;
    }

    const modal = document.createElement("div");
    modal.setAttribute("data-taxops-tour", "welcome");
    Object.assign(modal.style, {
      position: "fixed", inset: "0", zIndex: "9010",
      display: "flex", alignItems: "center", justifyContent: "center",
      pointerEvents: "all",
    });
    modal.innerHTML =
      `<div style="background:var(--bg-card,#fff);border:1px solid var(--border-default,#ddd);` +
             `border-radius:1rem;padding:2rem;max-width:440px;width:90vw;` +
             `box-shadow:0 20px 60px rgba(0,0,0,0.3);animation:tourFadeIn 220ms ease both;">` +
        `<div style="margin-bottom:1rem;">` +
          `<span style="display:inline-block;background:var(--color-primary,#c08040);color:#fff;` +
                       `font-size:11px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;` +
                       `padding:3px 10px;border-radius:99px;margin-bottom:.75rem;">${escHtml(t("tour_label") || "Guided Tour")}</span>` +
          `<h2 style="font-size:1.25rem;font-weight:700;margin:0 0 .25rem;color:var(--text-primary);">${escHtml(step.title)}</h2>` +
          `<p style="font-size:.875rem;color:var(--text-muted);margin:0;">${escHtml(step.subtitle || "")}</p>` +
        `</div>` +
        body +
      `</div>`;

    document.body.appendChild(modal);
    modal.querySelector("[data-taxops-tour='modal-start']").addEventListener("click", () => {
      _stopAutoAdvance();
      onNext();
    });
    const skipBtn = modal.querySelector("[data-taxops-tour='modal-skip']");
    if (skipBtn) skipBtn.addEventListener("click", () => { _stopAutoAdvance(); onSkip(); });
    const backBtn = modal.querySelector("[data-taxops-tour='modal-back']");
    if (backBtn && onBack) backBtn.addEventListener("click", () => { _stopAutoAdvance(); onBack(); });
    const presentBtn = modal.querySelector("[data-taxops-tour='modal-present']");
    if (presentBtn) presentBtn.addEventListener("click", () => {
      _stopAutoAdvance();
      _presentMode = true;
      _presentPaused = false;
      onNext();
    });
  }

  // ── Tooltip (steps 1–8) ──────────────────────────────────────────────────────

  function _tooltip(step, idx, total) {
    // Steps 0 and 1 are modals — exclude both from the progress dot count
    const MODAL_COUNT   = 2;
    const nonModalTotal = total - MODAL_COUNT;
    const dotIdx        = idx - MODAL_COUNT;   // 0-based among tooltip steps
    const isLast        = idx === total - 1;
    const showBack      = idx > MODAL_COUNT;   // back not shown on first tooltip
    const pres          = _presentMode;

    // Scale factors: presentation mode uses ~1.5× larger text and wider card
    const dotSz   = pres ? "9px"     : "6px";
    const ctrSz   = pres ? "15px"    : "11px";
    const titleSz = pres ? "1.35rem" : ".875rem";
    const bodySz  = pres ? "1.1rem"  : ".8125rem";
    const btnSz   = pres ? "1rem"    : ".8125rem";
    const btnPad  = pres ? "8px 20px" : "5px 14px";
    const bkPad   = pres ? "8px 16px" : "5px 12px";
    const closeSz = pres ? "22px"    : "15px";
    const cardW   = pres ? "min(580px,94vw)" : "min(340px,92vw)";
    const cardPad = pres ? "1.5rem 1.75rem"  : "1rem 1.125rem";

    const dots = Array.from({ length: nonModalTotal }, (_, i) =>
      `<span style="display:inline-block;width:${dotSz};height:${dotSz};border-radius:50%;` +
             `background:${i === dotIdx
               ? "var(--color-primary,#c08040)"
               : "var(--border-default,#d4c4b0)"};` +
             `transition:background 200ms;"></span>`
    ).join("");

    const tt = document.createElement("div");
    tt.setAttribute("data-taxops-tour", "tooltip");
    Object.assign(tt.style, {
      position: "fixed", zIndex: "9010",
      maxWidth: cardW, width: cardW,
      background: "var(--bg-card,#fff)",
      border: "1px solid var(--border-default,#ddd)",
      color: "var(--text-primary)",
      borderRadius: pres ? "1rem" : ".75rem",
      padding: cardPad,
      boxShadow: pres ? "0 20px 60px rgba(0,0,0,0.28)" : "0 8px 32px rgba(0,0,0,0.18)",
      opacity: "0", transition: "opacity 150ms ease",
      pointerEvents: "all",
      animation: "tourFadeIn 180ms ease both",
    });

    // Header row — counter + (pause in pres mode) + close
    const headerRight = pres
      ? `<div style="display:flex;gap:.5rem;align-items:center;">` +
          `<button data-taxops-tour="pause-play" title="Pause / resume (Space)" ` +
                  `style="font-size:16px;line-height:1;color:var(--text-muted);background:none;` +
                         `border:1px solid var(--border-default,#ddd);border-radius:.375rem;` +
                         `cursor:pointer;padding:2px 8px;">⏸</button>` +
          `<button data-taxops-tour="close-x" style="font-size:${closeSz};line-height:1;` +
                  `color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0 2px;" ` +
                  `title="End tour">&times;</button>` +
        `</div>`
      : `<button data-taxops-tour="close-x" style="font-size:${closeSz};line-height:1;` +
               `color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0 2px;" ` +
               `title="End tour">&times;</button>`;

    const countdownBar = pres
      ? `<div style="margin-top:${pres ? "1rem" : ".5rem"};height:5px;border-radius:99px;` +
               `background:var(--border-default,#e4d9cc);overflow:hidden;">` +
          `<div data-taxops-tour="countdown" ` +
               `style="height:100%;transform-origin:left;background:var(--color-primary,#c08040);` +
                      `animation:tourCountdown ${PRESENT_STEP_MS / 1000}s linear forwards;"></div>` +
        `</div>`
      : "";

    tt.innerHTML =
      `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:${pres ? ".875rem" : ".625rem"};">` +
        `<span style="font-size:${ctrSz};font-weight:600;color:var(--color-primary,#c08040);letter-spacing:.05em;text-transform:uppercase;">${idx - MODAL_COUNT + 1} of ${nonModalTotal}</span>` +
        headerRight +
      `</div>` +
      `<p style="font-weight:700;font-size:${titleSz};margin:0 0 ${pres ? ".625rem" : ".375rem"};color:var(--text-primary);line-height:1.3;">${escHtml(step.title)}</p>` +
      `<p style="font-size:${bodySz};line-height:1.65;margin:0 0 ${pres ? "1.25rem" : ".875rem"};color:var(--text-secondary,var(--text-muted));">${escHtml(step.body)}</p>` +
      `<div style="display:flex;justify-content:space-between;align-items:center;gap:.5rem;">` +
        `<div style="display:flex;gap:${pres ? "6px" : "4px"};align-items:center;">${dots}</div>` +
        `<div style="display:flex;gap:.5rem;align-items:center;">` +
          (showBack
            ? `<button data-taxops-tour="back" style="font-size:${btnSz};color:var(--text-muted);background:none;border:1px solid var(--border-default,#ddd);border-radius:.375rem;padding:${bkPad};cursor:pointer;">&larr; Back</button>`
            : "") +
          `<button data-taxops-tour="next" style="font-size:${btnSz};font-weight:600;background:var(--color-primary,#c08040);color:#fff;border:none;border-radius:.375rem;padding:${btnPad};cursor:pointer;">${isLast ? "Done &#10003;" : "Next &rarr;"}</button>` +
        `</div>` +
      `</div>` +
      (pres
        ? countdownBar
        : `<div style="text-align:center;margin-top:.5rem;">` +
            `<button data-taxops-tour="skip" style="font-size:11px;color:var(--text-muted);background:none;border:none;cursor:pointer;padding:0;opacity:.75;">End tour</button>` +
          `</div>`);

    document.body.appendChild(tt);
    requestAnimationFrame(() => { tt.style.opacity = "1"; });

    tt.querySelector("[data-taxops-tour='next']").addEventListener("click", () => {
      _stopAutoAdvance();
      if (isLast) complete(); else goToStep(idx + 1);
    });
    const backBtn = tt.querySelector("[data-taxops-tour='back']");
    if (backBtn) backBtn.addEventListener("click", () => { _stopAutoAdvance(); goToStep(Math.max(1, idx - 1)); });
    tt.querySelector("[data-taxops-tour='close-x']").addEventListener("click", () => complete());
    const skipBtn = tt.querySelector("[data-taxops-tour='skip']");
    if (skipBtn) skipBtn.addEventListener("click", () => complete());
    const pauseBtn = tt.querySelector("[data-taxops-tour='pause-play']");
    if (pauseBtn) pauseBtn.addEventListener("click", () => _togglePause());
    return tt;
  }

  // ── Tooltip positioning ──────────────────────────────────────────────────────

  function _position(tt, target) {
    const tr  = target.getBoundingClientRect();
    const ttH = tt.offsetHeight || 200;
    const ttW = tt.offsetWidth  || 340;
    const vp  = { w: window.innerWidth, h: window.innerHeight };
    const gap = 14;
    const top = (tr.bottom + ttH + gap < vp.h)
      ? tr.bottom + gap
      : Math.max(tr.top - ttH - gap, gap);
    const left = Math.min(Math.max(tr.left, gap), vp.w - ttW - gap);
    tt.style.top  = top  + "px";
    tt.style.left = left + "px";
  }

  // ── Navigation helpers ───────────────────────────────────────────────────────

  function _firstReturnUrl() {
    const a = document.querySelector("a[href^='/return/']");
    if (a) return a.getAttribute("href");
    // Dashboard rows use data-return-id (both server- and JS-rendered)
    const tr = document.querySelector("tr[data-return-id]") || document.querySelector("tr[data-id]");
    if (tr) return `/return/${tr.dataset.returnId || tr.dataset.id}`;
    return null;
  }

  function _firstClientUrl() {
    const a = document.querySelector("a[href^='/clients/']");
    return a ? a.getAttribute("href") : null;
  }

  // Fetch the first available return URL via the API, then navigate.
  // Falls back to DOM scraping first so common cases require no network round-trip.
  function _goToFirstReturn(idx) {
    const dom = _firstReturnUrl();
    if (dom) {
      _setResumeStep(idx);
      window.location.href = dom;
      return;
    }
    fetch("/api/tour/first-return", { credentials: "same-origin" })
      .then(r => r.json())
      .then(d => {
        if (d.url) {
          _setResumeStep(idx);
          window.location.href = d.url;
        } else {
          goToStep(idx + 1); // no returns in the system — skip gracefully
        }
      })
      .catch(() => goToStep(idx + 1));
  }

  function _navigateForStep(step, idx) {
    if (!step.navigate) return false;
    const path = window.location.pathname;

    if (step.navigate === "_first_return") {
      if (path.startsWith("/return/")) return false;
      _goToFirstReturn(idx);
      return true;

    } else if (step.navigate === "_first_client") {
      if (path.startsWith("/clients/")) return false;
      const nav = _firstClientUrl();
      if (nav) {
        _setResumeStep(idx);
        window.location.href = nav;
        return true;
      }
      // Need to reach a return detail page so the "Client profile →" link is visible
      if (!path.startsWith("/return/")) {
        _goToFirstReturn(idx);
        return true;
      }
      // On return detail but no client link — skip gracefully
      return false;

    } else {
      const nav = step.navigate;
      // Use exact match for "/" to avoid treating every path as "already at /"
      const alreadyHere = nav === "/" ? path === "/" : path.startsWith(nav);
      if (alreadyHere) return false;
      _setResumeStep(idx);
      window.location.href = nav;
      return true;
    }
  }

  // ── Step runner ──────────────────────────────────────────────────────────────

  function goToStep(idx) {
    const STEPS = _steps();
    if (idx >= STEPS.length) { complete(); return; }
    _currentStep = idx;
    const step = STEPS[idx];

    // Always clear any lingering modals or tooltips before rendering the new step
    document.querySelectorAll("[data-taxops-tour='tooltip'],[data-taxops-tour='welcome']").forEach(e => e.remove());

    // Modal steps (step 0 = welcome, step 1 = workflow, any future modal: true)
    if (step.modal) {
      const dismissModal = () => document.querySelectorAll("[data-taxops-tour='welcome']").forEach(e => e.remove());
      _showWelcomeModal(
        step,
        () => { dismissModal(); goToStep(idx + 1); },          // Next
        () => complete(),                                        // Skip (welcome only)
        idx > 0 ? () => { dismissModal(); goToStep(idx - 1); } : null  // Back (workflow+)
      );
      // Auto-advance modal steps in presentation mode
      if (_presentMode && !_presentPaused) {
        _startAutoAdvance(idx, PRESENT_MODAL_MS);
      }
      return;
    }

    // Navigate to a different page if needed
    if (_navigateForStep(step, idx)) return;

    // Remove previous tooltip + repos listener
    if (_reposOff) { _reposOff(); _reposOff = null; }
    document.querySelectorAll("[data-taxops-tour='tooltip']").forEach(e => e.remove());

    const target = step.selector ? document.querySelector(step.selector) : null;
    if (!target && step.selector) { goToStep(idx + 1); return; }  // skip missing targets

    if (target) _highlight(target);

    setTimeout(() => {
      const tt = _tooltip(step, idx, STEPS.length);
      if (target) {
        setTimeout(() => _position(tt, target), 20);
        const repos = () => _position(tt, target);
        window.addEventListener("scroll", repos, { passive: true });
        window.addEventListener("resize", repos, { passive: true });
        _reposOff = () => { window.removeEventListener("scroll", repos); window.removeEventListener("resize", repos); };
      } else {
        tt.style.top = "50%"; tt.style.left = "50%";
        tt.style.transform = "translate(-50%,-50%)";
      }
      // Auto-advance tooltip steps in presentation mode
      if (_presentMode && !_presentPaused) {
        _startAutoAdvance(idx, PRESENT_STEP_MS);
      }
    }, 80);
  }

  // ── Keyboard shortcuts ───────────────────────────────────────────────────────

  function _onKey(e) {
    if (!_active) return;
    if (e.key === "Escape")     { e.preventDefault(); complete(); }
    if (e.key === "ArrowRight") { e.preventDefault(); _stopAutoAdvance(); goToStep(_currentStep + 1); }
    if (e.key === "ArrowLeft" && _currentStep > 1) { e.preventDefault(); _stopAutoAdvance(); goToStep(_currentStep - 1); }
    if (e.key === " " && _presentMode) { e.preventDefault(); _togglePause(); }
  }

  // ── Public API ───────────────────────────────────────────────────────────────

  function start() {
    if (_active) return;
    _cleanup();        // _cleanup() sets _active=false — set it true AFTER
    _active = true;
    _backdrop();
    document.addEventListener("keydown", _onKey);
    goToStep(0);
  }

  function restart() {
    _cleanup();
    _active = true;
    sessionStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(PRESENT_KEY);
    _presentMode   = false;
    _presentPaused = false;
    _backdrop();
    document.addEventListener("keydown", _onKey);
    goToStep(0);
  }

  function startPresentation() {
    if (_active) return;
    _cleanup();
    _active        = true;
    _presentMode   = true;
    _presentPaused = false;
    _backdrop();
    document.addEventListener("keydown", _onKey);
    goToStep(0);
  }

  function dismiss() {
    sessionStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(PRESENT_KEY);
    _presentMode   = false;
    _presentPaused = false;
    document.removeEventListener("keydown", _onKey);
    _cleanup();
  }

  function complete() {
    sessionStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(PRESENT_KEY);
    _presentMode   = false;
    _presentPaused = false;
    document.removeEventListener("keydown", _onKey);
    _cleanup();
    _csrfFetch("/api/tour/complete", { method: "POST" }).catch(() => {});
  }

  // ── Resume from sessionStorage after page navigation ────────────────────────

  function _maybeResume() {
    const pending = sessionStorage.getItem(SESSION_KEY);
    if (pending === null) return;
    const idx = parseInt(pending, 10);
    if (isNaN(idx) || idx < 1 || idx >= _steps().length) {
      sessionStorage.removeItem(SESSION_KEY);
      sessionStorage.removeItem(PRESENT_KEY);
      return;
    }
    // Set _active immediately so any start() call that arrives before the
    // 500ms timeout hits the _active guard and bails out.
    _active = true;
    if (sessionStorage.getItem(PRESENT_KEY)) {
      _presentMode   = true;
      _presentPaused = false;
    }
    setTimeout(() => {
      sessionStorage.removeItem(SESSION_KEY);
      sessionStorage.removeItem(PRESENT_KEY);
      _backdrop();
      document.addEventListener("keydown", _onKey);
      goToStep(idx);
    }, 500);
  }

  document.addEventListener("DOMContentLoaded", _maybeResume);

  return { start, restart, startPresentation, dismiss, complete };

})();
