"""Lobby / kiosk office hours (Xcel Financial — Los Angeles).

Non-tax time: last new ticket at 5:00 PM Pacific.
Tax season (Jan 1–Apr 15 by default): no 5:00 cutoff — staff keep the line open later.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

DEFAULT_TZ = "America/Los_Angeles"
DEFAULT_TAX_SEASON_START = (1, 1)
DEFAULT_TAX_SEASON_END = (4, 15)
DEFAULT_NON_TAX_CLOSE = time(17, 0)
DEFAULT_NON_TAX_OPEN = time(9, 0)


def _pacific_offset_tz() -> timezone:
    """PDT/PST when tzdata is missing (common on Windows CPython)."""
    utc_now = datetime.now(timezone.utc)
    year = utc_now.year

    def nth_weekday(month: int, weekday: int, n: int) -> datetime:
        first = datetime(year, month, 1, tzinfo=timezone.utc)
        delta = (weekday - first.weekday()) % 7
        return first + timedelta(days=delta + 7 * (n - 1))

    # 2nd Sunday in March 10:00 UTC ≈ 02:00 PST; 1st Sunday in Nov 09:00 UTC ≈ 02:00 PDT.
    dst_start = nth_weekday(3, 6, 2).replace(hour=10)
    dst_end = nth_weekday(11, 6, 1).replace(hour=9)
    if dst_start <= utc_now < dst_end:
        return timezone(timedelta(hours=-7), name="PDT")
    return timezone(timedelta(hours=-8), name="PST")


def office_tz():
    name = (os.environ.get("TAXOPS_OFFICE_TZ") or DEFAULT_TZ).strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception:
        try:
            return ZoneInfo(DEFAULT_TZ)
        except Exception:
            return _pacific_offset_tz()


def _month_day(env_key: str, default: tuple[int, int]) -> tuple[int, int]:
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return default
    try:
        month_s, day_s = raw.split("-", 1)
        month, day = int(month_s), int(day_s)
        if 1 <= month <= 12 and 1 <= day <= 31:
            return month, day
    except ValueError:
        pass
    return default


def office_now(when: Optional[datetime] = None) -> datetime:
    tz = office_tz()
    if when is None:
        return datetime.now(tz)
    if when.tzinfo is None:
        return when.replace(tzinfo=tz)
    return when.astimezone(tz)


def is_tax_season(when: Optional[datetime] = None) -> bool:
    now = office_now(when)
    start = _month_day("TAXOPS_TAX_SEASON_START", DEFAULT_TAX_SEASON_START)
    end = _month_day("TAXOPS_TAX_SEASON_END", DEFAULT_TAX_SEASON_END)
    md = (now.month, now.day)
    return start <= md <= end


def _parse_hhmm(env_key: str, default: time) -> time:
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return default
    try:
        hour_s, minute_s = raw.split(":", 1)
        return time(int(hour_s), int(minute_s))
    except ValueError:
        return default


def non_tax_close_time() -> time:
    return _parse_hhmm("TAXOPS_NON_TAX_CLOSE", DEFAULT_NON_TAX_CLOSE)


def non_tax_open_time() -> time:
    return _parse_hhmm("TAXOPS_NON_TAX_OPEN", DEFAULT_NON_TAX_OPEN)


def clock_label(clock: time) -> str:
    hour = clock.hour % 12 or 12
    if clock.minute:
        return f"{hour}:{clock.minute:02d}"
    return f"{hour}:00"


def close_label(close: Optional[time] = None) -> str:
    return clock_label(close or non_tax_close_time())


def accepting_tickets(when: Optional[datetime] = None) -> bool:
    """True if the public kiosk may issue a new ticket."""
    if is_tax_season(when):
        return True
    now = office_now(when)
    opens = non_tax_open_time()
    close = non_tax_close_time()
    hm = (now.hour, now.minute)
    return hm >= (opens.hour, opens.minute) and hm < (close.hour, close.minute)


def lobby_hours(when: Optional[datetime] = None) -> dict[str, Any]:
    now = office_now(when)
    close = non_tax_close_time()
    opens = non_tax_open_time()
    tax = is_tax_season(now)
    open_now = accepting_tickets(now)
    close_txt = clock_label(close)
    open_txt = clock_label(opens)
    if tax:
        en, es = "Open today", "Abierto hoy"
        sub_en, sub_es = "Tax season hours", "Horario de temporada"
    elif open_now:
        en, es = f"Open until {close_txt}", f"Abierto hasta las {close_txt}"
        sub_en, sub_es = "", ""
    else:
        en, es = "Closed", "Cerrado"
        sub_en, sub_es = f"Opens at {open_txt}", f"Abrimos a las {open_txt}"
    return {
        "timezone": str(now.tzinfo),
        "tax_season": tax,
        "accepting_tickets": open_now,
        "close_label": close_txt,
        "open_label": open_txt,
        "local_time": now.strftime("%H:%M"),
        "en": en,
        "es": es,
        "sub_en": sub_en,
        "sub_es": sub_es,
    }
