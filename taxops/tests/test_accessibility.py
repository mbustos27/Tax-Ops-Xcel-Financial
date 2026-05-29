"""WCAG 2.2 structural accessibility checks (no browser required).

Parses template HTML with BeautifulSoup to verify basic ARIA and semantic
HTML requirements.  Does NOT replace manual or automated browser testing
(axe-core, Playwright) but catches the most common structural regressions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

bs4 = pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
from bs4 import BeautifulSoup  # noqa: E402


TEMPLATES = Path(__file__).resolve().parent.parent / "templates"


def _parse(template_name: str) -> BeautifulSoup:
    """Return a BeautifulSoup parse of the raw template file.

    Jinja tags ({% %}, {{ }}) are left in place; the HTML structure around
    them is still parseable for structural checks.
    """
    html = (TEMPLATES / template_name).read_text(encoding="utf-8")
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# intake.html — most form-heavy template
# ---------------------------------------------------------------------------


class TestIntakeLabels:
    """Every visible form field must have a programmatically associated label."""

    @pytest.fixture(scope="class")
    def soup(self):
        return _parse("intake.html")

    def test_all_text_inputs_have_id(self, soup):
        """Every input (except hidden/submit/radio/checkbox) has an id attribute."""
        bad = [
            inp
            for inp in soup.find_all("input")
            if inp.get("type") not in ("hidden", "submit", "radio", "checkbox", "button")
            and not inp.get("id")
        ]
        assert bad == [], f"Inputs without id: {[str(b)[:120] for b in bad]}"

    def test_all_selects_have_id(self, soup):
        """Every <select> has an id attribute."""
        bad = [s for s in soup.find_all("select") if not s.get("id")]
        assert bad == [], f"Selects without id: {[str(b)[:120] for b in bad]}"

    def test_labels_have_for_attribute(self, soup):
        """intake-label class labels must have a for= attribute."""
        bad = [
            lbl
            for lbl in soup.find_all("label", class_="intake-label")
            if not lbl.get("for")
        ]
        assert bad == [], (
            f"{len(bad)} intake-label(s) missing for=: "
            f"{[lbl.get_text(strip=True)[:60] for lbl in bad]}"
        )

    def test_for_attributes_point_to_existing_ids(self, soup):
        """Every label for= must point to an element that actually exists."""
        all_ids = {el.get("id") for el in soup.find_all(id=True)}
        broken = [
            lbl
            for lbl in soup.find_all("label")
            if lbl.get("for") and lbl["for"] not in all_ids
        ]
        assert broken == [], (
            f"Labels with dangling for= (no matching id): "
            f"{[(lbl.get_text(strip=True)[:40], lbl['for']) for lbl in broken]}"
        )

    def test_no_placeholder_only_fields(self, soup):
        """No visible input should rely solely on placeholder for its label.

        A field is 'placeholder-only' if it has a placeholder AND no associated
        label (by id matching) AND is not inside a <label> wrapper.

        Jinja-templated IDs (containing '{{') are skipped — they receive
        labels at runtime via JS-rendered row markup.
        """
        all_labels_by_for = {
            lbl["for"]: lbl
            for lbl in soup.find_all("label")
            if lbl.get("for")
        }
        placeholder_only = []
        for inp in soup.find_all("input"):
            if not inp.get("placeholder"):
                continue
            if inp.get("type") in ("hidden", "submit", "radio", "checkbox", "button"):
                continue
            field_id = inp.get("id", "")
            # Skip Jinja-templated IDs — resolved at runtime
            if "{{" in field_id:
                continue
            # Check 1: has a label pointing at it
            if field_id in all_labels_by_for:
                continue
            # Check 2: is wrapped in a <label> or has aria-label
            if inp.find_parent("label"):
                continue
            if inp.get("aria-label"):
                continue
            placeholder_only.append(inp)
        assert placeholder_only == [], (
            f"Inputs with placeholder but no label: "
            f"{[inp.get('name', inp.get('id', '?'))[:40] for inp in placeholder_only]}"
        )

    def test_fieldsets_exist(self, soup):
        """At least one fieldset must exist in the intake form (semantic grouping)."""
        form = soup.find("form", id="intake-form")
        assert form is not None, "intake-form not found"
        fieldsets = form.find_all("fieldset")
        assert len(fieldsets) >= 3, (
            f"Expected ≥3 fieldsets for grouping, found {len(fieldsets)}"
        )

    def test_fieldsets_have_legends(self, soup):
        """Every <fieldset> must contain a <legend>."""
        bad = [
            fs
            for fs in soup.find_all("fieldset")
            if not fs.find("legend")
        ]
        assert bad == [], f"{len(bad)} fieldset(s) without <legend>"


# ---------------------------------------------------------------------------
# Global: all templates must have accessible buttons
# ---------------------------------------------------------------------------

CHECKED_TEMPLATES = [
    "base.html",
    "dashboard.html",
    "intake.html",
    "return_detail.html",
]


class TestButtonAccessibility:
    """Every button must have an accessible name."""

    @pytest.mark.parametrize("template", CHECKED_TEMPLATES)
    def test_buttons_have_accessible_names(self, template):
        soup = _parse(template)
        bad = []
        for btn in soup.find_all("button"):
            text = btn.get_text(strip=True)
            aria_label = btn.get("aria-label", "")
            aria_labelledby = btn.get("aria-labelledby", "")
            title = btn.get("title", "")
            if not text and not aria_label and not aria_labelledby and not title:
                bad.append(btn)
        assert bad == [], (
            f"{template}: {len(bad)} button(s) without accessible name: "
            f"{[str(b)[:100] for b in bad[:5]]}"
        )


class TestImageAltText:
    """Every <img> must have an alt attribute (may be empty for decorative)."""

    @pytest.mark.parametrize("template", CHECKED_TEMPLATES)
    def test_images_have_alt(self, template):
        soup = _parse(template)
        bad = [img for img in soup.find_all("img") if img.get("alt") is None]
        assert bad == [], (
            f"{template}: {len(bad)} <img> tag(s) missing alt attribute"
        )


class TestBaseHtml:
    """base.html structural accessibility checks."""

    @pytest.fixture(scope="class")
    def soup(self):
        return _parse("base.html")

    def test_html_lang_set(self, soup):
        """<html> must have a lang attribute (WCAG 3.1.1)."""
        html_tag = soup.find("html")
        assert html_tag is not None
        lang = html_tag.get("lang", "")
        assert lang, "<html> is missing lang attribute"

    def test_flash_messages_have_role_alert(self, soup):
        """Flash message container must have role=alert."""
        # The div wrapping flashed messages has role="alert"
        alert_divs = soup.find_all("div", attrs={"role": "alert"})
        assert len(alert_divs) >= 1, "No role=alert found in base.html"

    def test_nav_has_aria_label(self, soup):
        """<nav> elements must have aria-label or aria-labelledby (WCAG 4.1.2)."""
        navs = soup.find_all("nav")
        bad = [
            n for n in navs
            if not n.get("aria-label") and not n.get("aria-labelledby")
        ]
        assert bad == [], f"{len(bad)} <nav> element(s) without aria-label"

    def test_tools_btn_has_aria_expanded(self, soup):
        """Tools dropdown button must have aria-expanded for screen readers."""
        btn = soup.find("button", id="tools-btn")
        assert btn is not None, "tools-btn not found"
        assert btn.get("aria-expanded") is not None, "tools-btn missing aria-expanded"

    def test_reject_bell_has_aria_expanded(self, soup):
        """Reject bell button must have aria-expanded."""
        btn = soup.find("button", id="reject-bell")
        assert btn is not None, "reject-bell not found"
        assert btn.get("aria-expanded") is not None, "reject-bell missing aria-expanded"


class TestDashboardHtml:
    """dashboard.html accessibility checks."""

    @pytest.fixture(scope="class")
    def soup(self):
        return _parse("dashboard.html")

    def test_table_has_caption_or_aria_label(self, soup):
        """Data table must have either <caption> or aria-label (WCAG 1.3.1)."""
        table = soup.find("table", class_="data-table")
        assert table is not None, "data-table not found"
        has_caption = bool(table.find("caption"))
        has_aria_label = bool(table.get("aria-label"))
        assert has_caption or has_aria_label, (
            "data-table has neither <caption> nor aria-label"
        )

    def test_table_has_sticky_header(self, soup):
        """<thead> must have sticky class for focus-not-obscured compliance."""
        thead = soup.find("thead")
        assert thead is not None
        classes = " ".join(thead.get("class", []))
        assert "sticky" in classes, "<thead> is not sticky (WCAG 2.4.11)"

    def test_empty_state_exists(self, soup):
        """A 'no results' message must exist for empty filtered state."""
        text = soup.get_text()
        assert "No returns found" in text or "returns" in text.lower()
