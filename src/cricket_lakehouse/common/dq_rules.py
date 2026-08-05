from __future__ import annotations


def legal_delivery(wides: int | None, noballs: int | None) -> bool:
    return int(wides or 0) == 0 and int(noballs or 0) == 0


def bowler_conceded(runs: dict[str, int] | None) -> int:
    runs = runs or {}
    return int(runs.get("total", 0) or 0) - int(runs.get("byes", 0) or 0) - int(
        runs.get("legbyes", 0) or 0
    ) - int(runs.get("penalty", 0) or 0)


def wicket_credits_bowler(kind: str | None) -> bool:
    return (kind or "").lower() not in {"run out", "retired hurt", "retired out", "obstructing the field"}


def compare_revision(current_revision: int | None, current_hash: str | None, new_revision: int | None, new_hash: str) -> str:
    if not current_hash:
        return "new"
    if current_hash == new_hash:
        return "unchanged"
    if new_revision is not None and current_revision is not None and new_revision < current_revision:
        return "older"
    return "revised"
