"""Tests for the supported Syntheca CLI surface."""

from __future__ import annotations

import pathlib

import polars as pl

from syntheca.cli import build_parser, main
from syntheca.reporting.output_groups import OutputGroupName
from syntheca.reporting.parity import validate_output_group_contract


def test_build_parser_exposes_supported_commands() -> None:
    parser = build_parser()
    subparsers_action = next(
        action for action in parser._actions if getattr(action, "dest", None) == "command"
    )
    assert {"run", "compare-scopus"}.issubset(subparsers_action.choices)


def test_compare_scopus_cli_writes_documented_output_group(tmp_path: pathlib.Path) -> None:
    fixture_root = pathlib.Path(__file__).parent / "fixtures"
    export_path = fixture_root / "comparison" / "scopus_export_sample.xlsx"
    internal_path = fixture_root / "merged" / "final_sample.parquet"

    exit_code = main(
        [
            "compare-scopus",
            str(export_path),
            "--internal-parquet",
            str(internal_path),
            "--output-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 0
    validation = validate_output_group_contract(tmp_path, OutputGroupName.COMPARISON)
    assert validation.is_valid is True


def test_python_module_entrypoint_contract_files_are_parquet_compatible(
    tmp_path: pathlib.Path,
) -> None:
    df = pl.DataFrame(
        {"doi": ["10.1/a"], "title": ["A"], "publication_year": [2024], "type": ["article"]}
    )
    df.write_parquet(tmp_path / "merged.parquet")
    df.write_excel(tmp_path / "merged.xlsx")

    validation = validate_output_group_contract(tmp_path, OutputGroupName.CORE_DATA)
    assert validation.is_valid is True
