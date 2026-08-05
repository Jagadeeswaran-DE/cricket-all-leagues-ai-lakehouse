from cricket_lakehouse.common.competition_config import competition_bucket
from cricket_lakehouse.common.dq_rules import (
    bowler_conceded,
    compare_revision,
    legal_delivery,
    wicket_credits_bowler,
)
from cricket_lakehouse.common.hash_utils import stable_match_id


def test_legal_ball_rules_and_bowler_concession():
    assert legal_delivery(0, 0)
    assert not legal_delivery(1, 0)
    assert not legal_delivery(0, 1)
    assert bowler_conceded({"batter": 4, "extras": 1, "byes": 1, "total": 5}) == 4


def test_wicket_credit_and_revision_rules():
    assert wicket_credits_bowler("caught")
    assert not wicket_credits_bowler("run out")
    assert compare_revision(1, "old", 2, "new") == "revised"
    assert compare_revision(2, "same", 2, "same") == "unchanged"


def test_stable_fallback_id_and_competition_detection():
    match_id, rule = stable_match_id({"info": {"teams": ["A", "B"]}})
    assert match_id.startswith("fallback_")
    assert rule == "canonical_sha256"
    assert competition_bucket("Indian Premier League 2025", ["Indian Premier League"])[1]
