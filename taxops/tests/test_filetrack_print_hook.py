"""M3 — the print hook wired into POST /intake, behind FILETRACK_ENABLED.
Must never break intake regardless of printer/module availability."""
from __future__ import annotations

from unittest.mock import patch


def test_intake_succeeds_when_filetrack_disabled_default(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/intake", data={"last_name": "Smith"}, follow_redirects=False)
    assert resp.status_code == 302


def test_intake_calls_print_label_when_enabled(client_logged_in, taxops_db_path, monkeypatch):
    monkeypatch.setattr("filetrack.config.FILETRACK_ENABLED", True)

    with patch("filetrack.labels.print_label.print_label") as mock_print:
        resp = client_logged_in.post("/intake", data={"last_name": "Jones"}, follow_redirects=False)

    assert resp.status_code == 302
    mock_print.assert_called_once()
    called_log_number = mock_print.call_args[0][0]
    assert called_log_number  # a non-empty log number was passed through


def test_intake_survives_print_failure(client_logged_in, taxops_db_path, monkeypatch):
    monkeypatch.setattr("filetrack.config.FILETRACK_ENABLED", True)

    with patch(
        "filetrack.labels.print_label.print_label",
        side_effect=RuntimeError("printer offline"),
    ):
        resp = client_logged_in.post("/intake", data={"last_name": "Garcia"}, follow_redirects=False)

    # Intake must still succeed (302 redirect to the new return) even though
    # the print attempt raised — printing is a convenience, not a hard invariant.
    assert resp.status_code == 302

    from db import get_connection
    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT id FROM clients WHERE last_name = 'GARCIA'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None


def test_intake_does_not_import_print_label_when_disabled(client_logged_in, taxops_db_path, monkeypatch):
    """When FILETRACK_ENABLED is false (default), the print hook must not
    even attempt to import filetrack.labels.print_label — a machine with no
    pywin32 installed must never see an import error from intake."""
    monkeypatch.setattr("filetrack.config.FILETRACK_ENABLED", False)

    with patch("filetrack.labels.print_label.print_label") as mock_print:
        resp = client_logged_in.post("/intake", data={"last_name": "Nguyen"}, follow_redirects=False)

    assert resp.status_code == 302
    mock_print.assert_not_called()


def test_intake_calls_relay_client_when_print_mode_is_relay(client_logged_in, taxops_db_path, monkeypatch):
    """FILETRACK_PRINT_MODE=relay must call print_label_via_relay(), never
    the direct win32print path — this is the fix for a Windows *service*
    that has no access to a printer on a different machine."""
    monkeypatch.setattr("filetrack.config.FILETRACK_ENABLED", True)
    monkeypatch.setattr("filetrack.config.FILETRACK_PRINT_MODE", "relay")

    with patch("filetrack.labels.relay_client.print_label_via_relay") as mock_relay, \
         patch("filetrack.labels.print_label.print_label") as mock_local:
        resp = client_logged_in.post("/intake", data={"last_name": "Alvarez"}, follow_redirects=False)

    assert resp.status_code == 302
    mock_relay.assert_called_once()
    mock_local.assert_not_called()


def test_intake_survives_relay_unreachable(client_logged_in, taxops_db_path, monkeypatch):
    monkeypatch.setattr("filetrack.config.FILETRACK_ENABLED", True)
    monkeypatch.setattr("filetrack.config.FILETRACK_PRINT_MODE", "relay")

    from filetrack.labels.relay_client import RelayError

    with patch(
        "filetrack.labels.relay_client.print_label_via_relay",
        side_effect=RelayError("could not reach print relay"),
    ):
        resp = client_logged_in.post("/intake", data={"last_name": "Ibarra"}, follow_redirects=False)

    assert resp.status_code == 302
