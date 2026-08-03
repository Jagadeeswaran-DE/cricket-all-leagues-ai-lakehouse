from cricket_lakehouse.common.config import LakehouseConfig


def test_table_uses_three_level_unity_catalog_name() -> None:
    config = LakehouseConfig(catalog="cricket", schema="cricket_all")

    assert config.table("silver_deliveries") == "cricket.cricket_all.silver_deliveries"


def test_table_supports_smoke_prefix() -> None:
    config = LakehouseConfig(catalog="cricket", schema="cricket_all", table_prefix="smoke_")

    assert config.table("silver_deliveries") == "cricket.cricket_all.smoke_silver_deliveries"

