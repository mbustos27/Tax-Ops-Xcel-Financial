"""Non-tax close is 5:00 PM Pacific; tax season has no 5:00 cutoff."""
from __future__ import annotations

from datetime import datetime

from zoneinfo import ZoneInfo

from office_hours import accepting_tickets, is_tax_season, lobby_hours, office_tz

PT = ZoneInfo("America/Los_Angeles")


def test_office_tz_always_resolves():
    tz = office_tz()
    assert tz is not None
    datetime.now(tz)


def test_tax_season_is_jan_through_apr_15():
    assert is_tax_season(datetime(2026, 1, 1, 8, 0, tzinfo=PT))
    assert is_tax_season(datetime(2026, 4, 15, 23, 0, tzinfo=PT))
    assert not is_tax_season(datetime(2026, 4, 16, 8, 0, tzinfo=PT))
    assert not is_tax_season(datetime(2026, 9, 21, 16, 0, tzinfo=PT))


def test_non_tax_close_at_5pm_pacific():
    open_at = datetime(2026, 9, 21, 16, 59, tzinfo=PT)
    closed_at = datetime(2026, 9, 21, 17, 0, tzinfo=PT)
    assert accepting_tickets(open_at) is True
    assert accepting_tickets(closed_at) is False
    hours = lobby_hours(closed_at)
    assert hours["tax_season"] is False
    assert hours["accepting_tickets"] is False
    assert hours["en"] == "Closed"
    assert hours["es"] == "Cerrado"
    assert hours["sub_en"] == "Opens at 9:00"
    assert hours["sub_es"] == "Abrimos a las 9:00"
    assert hours["close_label"] == "5:00"


def test_non_tax_before_open_is_closed():
    hours = lobby_hours(datetime(2026, 9, 21, 8, 30, tzinfo=PT))
    assert hours["accepting_tickets"] is False
    assert hours["en"] == "Closed"
    assert hours["sub_en"] == "Opens at 9:00"


def test_non_tax_afternoon_shows_open_until_5():
    hours = lobby_hours(datetime(2026, 9, 21, 14, 30, tzinfo=PT))
    assert hours["accepting_tickets"] is True
    assert hours["en"] == "Open until 5:00"
    assert hours["es"] == "Abierto hasta las 5:00"


def test_tax_season_stays_open_after_5():
    late = datetime(2026, 4, 1, 19, 30, tzinfo=PT)
    assert accepting_tickets(late) is True
    hours = lobby_hours(late)
    assert hours["tax_season"] is True
    assert hours["en"] == "Open today"
    assert hours["es"] == "Abierto hoy"
    assert hours["sub_en"] == "Tax season hours"
