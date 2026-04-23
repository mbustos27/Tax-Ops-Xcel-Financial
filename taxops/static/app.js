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
  container.innerHTML = items.map(r => `
    <a href="/return/${r.id}"
       class="flex items-center gap-3 px-4 py-2.5 hover:bg-slate-50 transition-colors border-b border-slate-100 last:border-0">
      <span class="font-mono font-bold text-slate-400 w-10 shrink-0 text-xs">${r.log_number ?? '—'}</span>
      <span class="flex-1 min-w-0">
        <span class="block text-sm font-semibold text-slate-800">${r.name}</span>
        <span class="text-xs text-slate-400">TY${r.tax_year ?? '—'}</span>
      </span>
      <span class="text-xs px-2 py-px rounded-full border shrink-0 ${r.badge || "bg-slate-100 text-slate-500 border-slate-200"}">${r.status || "—"}</span>
    </a>
  `).join("");
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
    const resp = await fetch(`/api/return/${returnId}/status`, {
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
    if (e.key === "Escape") { el.textContent = el.dataset.original || "—"; }
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
  if (unchanged) { el.textContent = original || "—"; return; }

  try {
    const resp = await fetch(`/api/return/${returnId}/field`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ field, value: value || null }),
    });
    const data = await resp.json();
    el.textContent    = value || "—";
    el.dataset.value  = value;
    if (data.success) flash(el);
  } catch {
    el.textContent = original || "—";
  }
}

async function toggleBool(el) {
  const returnId = el.dataset.returnId;
  const field    = el.dataset.field;
  const isTrue   = el.dataset.value === "1";
  const newVal   = isTrue ? 0 : 1;
  try {
    const resp = await fetch(`/api/return/${returnId}/field`, {
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
    const resp = await fetch(`/api/return/${returnId}/note`, {
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

function scheduleTableQuickFilter(immediate) {
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

// ── Init ─────────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  initSearch();
  initInlineEdit();
  initTableFilter();
  initDashboardTableSelection();
  initYearPicker();

  // Press "/" to focus search from anywhere
  document.addEventListener("keydown", e => {
    const tag = document.activeElement?.tagName;
    if (e.key === "/" && tag !== "INPUT" && tag !== "TEXTAREA" && tag !== "SELECT") {
      e.preventDefault();
      document.getElementById("global-search")?.focus();
    }
  });
});
