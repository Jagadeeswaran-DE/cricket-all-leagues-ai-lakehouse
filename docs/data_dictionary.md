# Data Dictionary

| Layer | Table | Grain | Key fields | Chatbot use |
|---|---|---|---|---|
| Bronze | `bronze_raw_matches` | latest match revision | `match_id` | provenance and fallback retrieval |
| Bronze | `bronze_raw_match_versions` | received match version | `match_id`, `source_file_sha256` | revision audit |
| Silver | `silver_matches` | match | `match_id` | result, teams, competition, venue |
| Silver | `silver_deliveries` | delivery | `match_id`, `innings_number`, `over_number`, `delivery_sequence` | ball-level reasoning |
| Silver | `silver_wickets` | wicket | delivery key plus wicket index | dismissal reasoning |
| Silver | `silver_player_registry` | person per match | `match_id`, `person_id` | reliable identity resolution |
| Gold | `gold_player_batting_stats` | player/league/season | dimensions plus player | batting answers |
| Gold | `gold_bowler_stats` | bowler/league/season | dimensions plus bowler | bowling answers |
| Gold | `gold_team_match_summary` | team innings | `match_id`, batting team | score and run-rate answers |
| Gold | `gold_league_season_summary` | league/season | league dimensions | comparison answers |
| Serving | `gold_ai_match_facts_*` | match | `match_id` plus segment | low-latency match retrieval |
| Serving | `gold_ai_player_cards_*` | player/league/season | player plus dimensions | player cards and comparison |
| Serving | `gold_ai_team_season_cards_*` | team/league/season | team plus dimensions | team trends |

Use Silver deliveries as the canonical scoring source. Gold is an aggregation
layer, not a replacement for provenance.
