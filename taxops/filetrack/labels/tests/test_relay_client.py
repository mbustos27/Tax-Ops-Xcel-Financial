"""filetrack.labels.relay_client — POST-to-relay print path, mocking
`requests` so this stays runnable without a real relay process. Mirrors
filetrack/listener/tests/test_http_sink.py's mocking style."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from filetrack.labels.relay_client import RelayError, print_label_via_relay


def _resp(status_code, text=""):
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    return r


def test_success_posts_log_number_and_token():
    with patch("requests.post", return_value=_resp(200)) as mock_post:
        print_label_via_relay("1234", relay_url="http://relay:8765/print", token="tok")
    args, kwargs = mock_post.call_args
    assert args[0] == "http://relay:8765/print"
    assert kwargs["json"] == {"log_number": "1234"}
    assert kwargs["headers"]["X-Filetrack-Token"] == "tok"


def test_no_token_omits_header():
    with patch("requests.post", return_value=_resp(200)) as mock_post:
        print_label_via_relay("1234", relay_url="http://relay:8765/print", token="")
    _, kwargs = mock_post.call_args
    assert "X-Filetrack-Token" not in kwargs["headers"]


def test_non_200_raises_relay_error():
    with patch("requests.post", return_value=_resp(500, "printer offline")):
        with pytest.raises(RelayError):
            print_label_via_relay("1234", relay_url="http://relay:8765/print", token="tok")


def test_network_failure_raises_relay_error_not_original_exception():
    with patch("requests.post", side_effect=ConnectionError("host unreachable")):
        with pytest.raises(RelayError):
            print_label_via_relay("1234", relay_url="http://relay:8765/print", token="tok")


def test_defaults_come_from_filetrack_config(monkeypatch):
    monkeypatch.setattr("filetrack.labels.relay_client.FILETRACK_RELAY_URL", "http://default-relay/print")
    monkeypatch.setattr("filetrack.labels.relay_client.FILETRACK_RELAY_TOKEN", "default-tok")
    monkeypatch.setattr("filetrack.labels.relay_client.FILETRACK_RELAY_TIMEOUT_SEC", 3.0)

    with patch("requests.post", return_value=_resp(200)) as mock_post:
        print_label_via_relay("1234")

    args, kwargs = mock_post.call_args
    assert args[0] == "http://default-relay/print"
    assert kwargs["headers"]["X-Filetrack-Token"] == "default-tok"
    assert kwargs["timeout"] == 3.0
