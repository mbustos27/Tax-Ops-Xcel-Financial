"""Tests for I18N-1 through I18N-6 — Flask-Babel Spanish (Mexico) language mode."""
import json


# ── I18N-1: /set-language ─────────────────────────────────────────────────────

class TestSetLanguage:
    def test_requires_login(self, client):
        r = client.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        assert r.status_code in (302, 401, 403)

    def test_switch_to_es_mx(self, client_logged_in):
        r = client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data["success"] is True
        assert data["locale"] == "es_MX"

    def test_switch_back_to_en(self, client_logged_in):
        client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        r = client_logged_in.post(
            "/set-language",
            json={"locale": "en"},
            content_type="application/json",
        )
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data["locale"] == "en"

    def test_invalid_locale_returns_400(self, client_logged_in):
        r = client_logged_in.post(
            "/set-language",
            json={"locale": "fr"},
            content_type="application/json",
        )
        assert r.status_code == 400
        data = json.loads(r.data)
        assert "error" in data

    def test_unsupported_bare_es_returns_400(self, client_logged_in):
        """Bare 'es' is not a supported locale — only es_MX."""
        r = client_logged_in.post(
            "/set-language",
            json={"locale": "es"},
            content_type="application/json",
        )
        assert r.status_code == 400

    def test_missing_locale_field_returns_400(self, client_logged_in):
        r = client_logged_in.post(
            "/set-language",
            json={"language": "es_MX"},
            content_type="application/json",
        )
        assert r.status_code == 400


# ── I18N-4: /api/translations ────────────────────────────────────────────────

class TestApiTranslations:
    def test_returns_200_unauthenticated(self, client):
        r = client.get("/api/translations")
        assert r.status_code == 200

    def test_english_default(self, client):
        r = client.get("/api/translations")
        data = json.loads(r.data)
        assert data["loading"] == "Loading..."
        assert data["no_results"] == "No results"
        assert data["tour_s1_title"] == "Find any client instantly"

    def test_spanish_mx_after_toggle(self, client_logged_in):
        client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        r = client_logged_in.get("/api/translations")
        data = json.loads(r.data)
        assert data["loading"] == "Cargando..."
        assert data["no_results"] == "Sin resultados"
        assert data["tour_s1_title"] == "Encuentra a cualquier cliente al instante"

    def test_has_all_required_keys(self, client):
        r = client.get("/api/translations")
        data = json.loads(r.data)
        required = [
            "loading", "saving", "uploading", "confirm_delete", "no_results",
            "error_generic", "upload_success", "upload_error", "doc_deleted",
            "classification_saved", "session_expired",
            "tour_s1_title", "tour_s1_body",
            "tour_s2_title", "tour_s2_body",
            "tour_s3_title", "tour_s3_body",
            "tour_s4_title", "tour_s4_body",
            "tour_s5_title", "tour_s5_body",
            "tour_s6_title", "tour_s6_body",
            "tour_s7_title", "tour_s7_body",
        ]
        for key in required:
            assert key in data, f"Missing key: {key}"

    def test_spanish_tour_step_7(self, client_logged_in):
        client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        r = client_logged_in.get("/api/translations")
        data = json.loads(r.data)
        assert data["tour_s7_title"] == "Estás listo"

    def test_back_to_english_after_switch(self, client_logged_in):
        client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        client_logged_in.post(
            "/set-language",
            json={"locale": "en"},
            content_type="application/json",
        )
        r = client_logged_in.get("/api/translations")
        data = json.loads(r.data)
        assert data["loading"] == "Loading..."


# ── I18N-1: locale in session ────────────────────────────────────────────────

class TestLocaleSession:
    def test_session_locale_defaults_to_en(self, client):
        """Without setting locale, GET /api/translations should return English."""
        r = client.get("/api/translations")
        data = json.loads(r.data)
        assert data["no_results"] == "No results"

    def test_locale_persists_across_requests(self, client_logged_in):
        client_logged_in.post(
            "/set-language",
            json={"locale": "es_MX"},
            content_type="application/json",
        )
        r = client_logged_in.get("/api/translations")
        data = json.loads(r.data)
        assert data["no_results"] == "Sin resultados"
