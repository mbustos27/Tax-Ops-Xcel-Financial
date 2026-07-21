"""M3 wiring — filetrack.listener.http_sink retry/backoff, mocking `requests`
so this stays runnable without a real TaxOps server."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from filetrack.listener.http_sink import build_http_sink


def _resp(status_code, text=""):
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    return r


def test_success_on_first_attempt_does_not_retry():
    with patch("requests.post", return_value=_resp(200)) as mock_post:
        sink = build_http_sink(url="http://x/filetrack/status", token="tok", backoff_seconds=0.01)
        sink.submit("00123", "FINALIZE", "2026-01-01T00:00:00+00:00")
    assert mock_post.call_count == 1
    _, kwargs = mock_post.call_args
    assert kwargs["headers"]["X-Filetrack-Token"] == "tok"
    assert kwargs["json"]["log_number"] == "00123"
    assert kwargs["json"]["status"] == "FINALIZE"


def test_4xx_response_is_not_retried():
    with patch("requests.post", return_value=_resp(400, "bad status")) as mock_post:
        sink = build_http_sink(url="http://x/filetrack/status", backoff_seconds=0.01, max_attempts=4)
        sink.submit("00123", "BOGUS", "ts")
    assert mock_post.call_count == 1


def test_5xx_response_retries_up_to_max_attempts():
    with patch("requests.post", return_value=_resp(500, "server error")) as mock_post:
        sink = build_http_sink(url="http://x/filetrack/status", backoff_seconds=0.001, max_attempts=3)
        sink.submit("00123", "FINALIZE", "ts")
    assert mock_post.call_count == 3


def test_network_exception_retries_then_gives_up_without_raising():
    with patch("requests.post", side_effect=ConnectionError("down")) as mock_post:
        sink = build_http_sink(url="http://x/filetrack/status", backoff_seconds=0.001, max_attempts=3)
        # Must never raise out of submit() — run_listener's loop depends on this.
        sink.submit("00123", "FINALIZE", "ts")
    assert mock_post.call_count == 3


def test_recovers_after_transient_failure():
    with patch(
        "requests.post",
        side_effect=[ConnectionError("down"), _resp(200)],
    ) as mock_post:
        sink = build_http_sink(url="http://x/filetrack/status", backoff_seconds=0.001, max_attempts=3)
        sink.submit("00123", "FINALIZE", "ts")
    assert mock_post.call_count == 2


def test_backoff_wait_grows_between_retries():
    sleeps = []
    with patch("requests.post", side_effect=ConnectionError("down")), \
         patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
        sink = build_http_sink(url="http://x/filetrack/status", backoff_seconds=1.0, max_attempts=4)
        sink.submit("00123", "FINALIZE", "ts")
    assert sleeps == [1.0, 2.0, 4.0]
