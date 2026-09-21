"""Lobby / kiosk office hours (Xcel Financial — Los Angeles).

Non-tax time: last new ticket at 5:00 PM Pacific.
Tax season (Jan 1–Apr 15 by default): no 5:00 cutoff — staff keep the line open later.
"""
from __future__ import annotations

import os
from datetime import datetime, time
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TZ = "America/Los_Angeles"
DEFAULT_TAX_SEASON_START = (1, 1)
DEFAULT_TAX_SEASON_END = (4, 15)
DEFAULT_NON_TAX_CLOSE = time(17, 0)


def office_tz() -> ZoneInfo:
    name = (os.environ.get("TAXOPS_OFFICE_TZ") or DEFAULT_TZ).strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_TZ)


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


def non_tax_close_time() -> time:
    raw = (os.environ.get("TAXOPS_NON_TAX_CLOSE") or "17:00").strip()
    try:
        hour_s, minute_s = raw.split(":", 1)
        hour, minute = int(hour_s), int(minute_s)
        return time(hour, minute)
    except ValueError:
        return DEFAULT_NON_TAX_CLOSE


def close_label(close: Optional[time] = None) -> str:
    close = close or non_tax_close_time()
    hour = close.hour % 12 or 12
    if close.minute:
        return f"{hour}:{close.minute:02d}"
    return f"{hour}:00"


def accepting_tickets(when: Optional[datetime] = None) -> bool:
    """True if the public kiosk may issue a new ticket."""
    if is_tax_season(when):
        return True
    now = office_now(when)
    close = non_tax_close_time()
    return (now.hour, now.minute) < (close.hour, close.minute)


def lobby_hours(when: Optional[datetime] = None) -> dict[str, Any]:
    now = office_now(when)
    close = non_tax_close_time()
    tax = is_tax_season(now)
    open_now = accepting_tickets(now)
    label = close_label(close)
    if tax:
        en, es = "", ""
    elif open_now:
        en, es = f"Open until {label}", f"Abierto hasta las {label}"
    else:
        en, es = "Closed", "Cerrado"
    return {
        "timezone": str(now.tzinfo),
        "tax_season": tax,
        "accepting_tickets": open_now,
        "close_label": label,
        "local_time": now.strftime("%H:%M"),
        "en": en,
        "es": es,
    }
