"""DEBT-6: schema version tracking — db.py and /health surface version."""
from __future__ import annotations

import pytest


def test_current_schema_version_is_positive_int():
    """CURRENT_SCHEMA_VERSION is a positive integer."""
    from db import CURRENT_SCHEMA_VERSION
    assert isinstance(CURRENT_SCHEMA_VERSION, int)
    assert CURRENT_SCHEMA_VERSION >= 1


def test_get_set_schema_version(taxops_db_path):
    """set_schema_version / get_schema_version roundtrip in app_settings."""
    from db import get_connection, get_schema_version, set_schema_version

    conn = get_connection(taxops_db_path)
    try:
        set_schema_version(conn, 42)
        conn.commit()
        assert get_schema_version(conn) == 42
    finally:
        conn.close()


def test_init_db_sets_schema_version(taxops_db_path):
    """init_db writes CURRENT_SCHEMA_VERSION into app_settings."""
    from db import get_connection, get_schema_version, CURRENT_SCHEMA_VERSION

    conn = get_connection(taxops_db_path)
    try:
        version = get_schema_version(conn)
        assert version == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_health_exposes_schema_version(client):
    """/health JSON includes schema_version and schema_version_expected."""
    resp = client.get("/health")
    assert resp.status_code in (200, 503)
    data = resp.get_json()
    assert "schema_version" in data, f"schema_version missing from /health: {data}"
    assert "schema_version_expected" in data
    assert isinstance(data["schema_version"], int)
    assert isinstance(data["schema_version_expected"], int)


def test_health_schema_version_matches_expected(client):
    """/health schema_version equals schema_version_expected after a fresh init."""
    resp = client.get("/health")
    data = resp.get_json()
    assert data["schema_version"] == data["schema_version_expected"], (
        f"Schema mismatch: got {data['schema_version']} expected {data['schema_version_expected']}"
    )


def test_get_schema_version_returns_zero_when_missing(tmp_path):
    """get_schema_version returns 0 when key not present (safe bootstrap)."""
    import config as cfg
    import db as db_mod

    db_path = str(tmp_path / "empty.db")
    conn = db_mod.get_connection(db_path)
    # Create app_settings table but don't insert schema_version key.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)"
    )
    conn.commit()
    ver = db_mod.get_schema_version(conn)
    conn.close()
    assert ver == 0
