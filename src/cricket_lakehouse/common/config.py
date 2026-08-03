from __future__ import annotations

import argparse
from dataclasses import dataclass

DEFAULT_SOURCE_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/extracted/*.json"


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str
    schema: str
    table_prefix: str = ""
    source_path: str = DEFAULT_SOURCE_PATH

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}{name}"


def parse_args(include_source_path: bool = False) -> LakehouseConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--table-prefix", default="")
    if include_source_path:
        parser.add_argument("--source-path", default=DEFAULT_SOURCE_PATH)
    args = parser.parse_args()

    return LakehouseConfig(
        catalog=args.catalog,
        schema=args.schema,
        table_prefix=args.table_prefix,
        source_path=getattr(args, "source_path", DEFAULT_SOURCE_PATH),
    )
