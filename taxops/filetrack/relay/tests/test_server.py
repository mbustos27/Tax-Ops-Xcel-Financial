"""filetrack.relay.server — auth + dispatch logic, mocking print_label() so
these run without a real printer or pywin32. Mirrors the auth-model tests in
tests/test_filetrack_endpoint.py (token-if-set, localhost-fallback-if-not)."""
from __future__ import annotations

from unittest.mock import patch

from filetrack.relay.server import handle_print_job


def _call(payload, *, token_header="", expected_token="", remote_addr="10.0.0.9"):
    return handle_print_job(
        payload,
        token_header=token_header,
        expected_token=expected_token,
        remote_addr=remote_addr,
    )


def test_missing_log_number_returns_400():
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status, body = _call({}, expected_token="", remote_addr="127.0.0.1")
    assert status == 400
    assert "error" in body
    mock_print.assert_not_called()


def test_raw_zpl_prints_without_log_number():
    with patch("filetrack.labels.printer.send_zpl") as mock_send:
        status, body = _call(
            {"zpl": "^XA^FO0,0^FDTEST^FS^XZ"},
            expected_token="",
            remote_addr="127.0.0.1",
        )
    assert status == 200
    assert body["success"] is True
    assert body["mode"] == "raw_zpl"
    mock_send.assert_called_once()


def test_raw_zpl_rejects_non_zpl():
    with patch("filetrack.labels.printer.send_zpl") as mock_send:
        status, body = _call(
            {"zpl": "not a label"},
            expected_token="",
            remote_addr="127.0.0.1",
        )
    assert status == 400
    mock_send.assert_not_called()


def test_no_token_configured_allows_localhost_and_prints():
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status, body = _call({"log_number": "1234"}, expected_token="", remote_addr="127.0.0.1")
    assert status == 200
    assert body["success"] is True
    assert body["log_number"] == "1234"
    mock_print.assert_called_once()
    assert mock_print.call_args[0][0] == "1234"


def test_no_token_configured_rejects_non_localhost():
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status, body = _call({"log_number": "1234"}, expected_token="", remote_addr="192.168.1.141")
    assert status == 401
    mock_print.assert_not_called()


def test_token_configured_requires_matching_header():
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status_wrong, _ = _call(
            {"log_number": "1234"}, token_header="nope", expected_token="s3cr3t", remote_addr="192.168.1.141"
        )
        status_ok, body_ok = _call(
            {"log_number": "1234"}, token_header="s3cr3t", expected_token="s3cr3t", remote_addr="192.168.1.141"
        )
    assert status_wrong == 401
    assert status_ok == 200
    assert body_ok["success"] is True
    mock_print.assert_called_once()
    assert mock_print.call_args[0][0] == "1234"


def test_token_configured_allows_non_localhost_with_correct_token():
    """The whole point of relay mode: the caller (TaxOps server) is a
    DIFFERENT machine than the relay, so localhost-only would never work
    once a real token is configured."""
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status, body = _call(
            {"log_number": "5678"}, token_header="prod-secret", expected_token="prod-secret",
            remote_addr="192.168.1.141",
        )
    assert status == 200
    assert body["success"] is True
    mock_print.assert_called_once()
    assert mock_print.call_args[0][0] == "5678"


def test_print_passes_client_name_fields():
    with patch("filetrack.labels.print_label.print_label") as mock_print:
        status, body = _call(
            {"log_number": "99", "last_name": "Smith", "first_name": "Ann"},
            expected_token="",
            remote_addr="127.0.0.1",
        )
    assert status == 200
    assert mock_print.call_args.kwargs["last_name"] == "Smith"
    assert mock_print.call_args.kwargs["first_name"] == "Ann"


def test_printer_not_found_returns_500_but_does_not_raise():
    from filetrack.labels.printer import PrinterNotFoundError

    with patch(
        "filetrack.labels.print_label.print_label",
        side_effect=PrinterNotFoundError("no such printer"),
    ):
        status, body = _call({"log_number": "1234"}, expected_token="", remote_addr="127.0.0.1")
    assert status == 500
    assert "error" in body


def test_unexpected_exception_returns_500_but_does_not_raise():
    with patch(
        "filetrack.labels.print_label.print_label",
        side_effect=RuntimeError("something else broke"),
    ):
        status, body = _call({"log_number": "1234"}, expected_token="", remote_addr="127.0.0.1")
    assert status == 500
    assert "error" in body


def test_flask_app_print_endpoint_end_to_end():
    """create_app() wiring (Flask -> handle_print_job) with a real test client."""
    from filetrack.relay.server import create_app

    app = create_app(token="tok123")
    client = app.test_client()

    with patch("filetrack.labels.print_label.print_label") as mock_print:
        resp = client.post(
            "/print",
            json={"log_number": "999"},
            headers={"X-Filetrack-Token": "tok123"},
        )
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    mock_print.assert_called_once()
    assert mock_print.call_args[0][0] == "999"


def test_flask_app_health_endpoint():
    from filetrack.relay.server import create_app

    app = create_app(token="tok123")
    client = app.test_client()
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_flask_app_rejects_bad_token():
    from filetrack.relay.server import create_app

    app = create_app(token="tok123")
    client = app.test_client()

    with patch("filetrack.labels.print_label.print_label") as mock_print:
        resp = client.post("/print", json={"log_number": "999"}, headers={"X-Filetrack-Token": "wrong"})
    assert resp.status_code == 401
    mock_print.assert_not_called()
