from pathlib import Path

# Fix rejected dropdown href in base.html
p = Path(r"T:\taxops\templates\base.html")
t = p.read_text(encoding="utf-8")
old = '''                <a href="/return/{{ r.id }}"
                   class="flex items-start gap-3 px-4 py-3 transition-colors group"'''
new = '''                <a href="{{ return_open_path(r.id) }}"
                   class="flex items-start gap-3 px-4 py-3 transition-colors group"'''
if old not in t:
    print("rejected link: already done or missing")
else:
    t = t.replace(old, new, 1)
    print("rejected link: patched")

marker = """    })();\n  </script>\n  <script>\n    (function () {\n      const bell     = document.getElementById('reject-bell');"""
prep_js = r"""    })();
  </script>
  <script>
    (function () {
      const btn = document.getElementById('prep-mode-toggle');
      if (!btn) return;

      function renderState(enabled) {
        btn.dataset.enabled = enabled ? '1' : '0';
        btn.textContent = enabled ? 'Prep: ON' : 'Prep: OFF';
        if (enabled) {
          btn.className = 'text-xs font-medium rounded px-2 py-1 border transition-colors whitespace-nowrap bg-violet-100 text-violet-900 border-violet-300 hover:bg-violet-200';
          btn.style.background = '';
          btn.style.color = '';
          btn.style.borderColor = '';
        } else {
          btn.className = 'text-xs font-medium rounded px-2 py-1 border transition-colors whitespace-nowrap';
          btn.style.background = 'var(--nav-input-bg)';
          btn.style.color = 'var(--nav-text)';
          btn.style.borderColor = 'var(--border-strong)';
        }
      }

      renderState(btn.dataset.enabled === '1');

      btn.addEventListener('click', async () => {
        const current = btn.dataset.enabled === '1';
        try {
          const resp = await _csrfFetch('/api/prep-mode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled: !current }),
          });
          const data = await _authJson(resp);
          if (data.success) {
            renderState(!!data.prep_mode);
            window.location.reload();
          }
        } catch (e) {
          if (!e || e.message !== 'Unauthorized') {
            console.error('Prep mode toggle failed', e);
          }
        }
      });
    })();
  </script>
  <script>
    (function () {
      const bell     = document.getElementById('reject-bell');"""

if "prep-mode-toggle');" in t and "api/prep-mode" in t:
    print("prep js: already present")
elif marker not in t:
    print("prep js marker missing")
else:
    t = t.replace(marker, prep_js, 1)
    print("prep js: patched")

p.write_text(t, encoding="utf-8")
print("done")
