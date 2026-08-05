from __future__ import annotations

import re

DEFAULT_FOCUS_LEAGUES = (
    "Indian Premier League",
    "Big Bash League",
    "Women's Big Bash League",
    "The Hundred",
    "Women's Premier League",
    "Pakistan Super League",
    "Caribbean Premier League",
    "Major League Cricket",
    "International League T20",
    "SA20",
)


def parse_csv(value: str | None, default: tuple[str, ...] = ()) -> list[str]:
    values = [item.strip() for item in (value or "").split(",") if item.strip()]
    return values or list(default)


def normalise_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def competition_bucket(event_name: str | None, configured_focus: list[str]) -> tuple[str, bool]:
    """Classify by event name; the source filename is intentionally irrelevant."""
    event = normalise_name(event_name)
    for configured in configured_focus:
        if normalise_name(configured) in event or event in normalise_name(configured):
            return configured, True
    return event_name or "Unknown", False


def focus_predicate(event_name: str | None, configured_focus: list[str]) -> bool:
    return competition_bucket(event_name, configured_focus)[1]
