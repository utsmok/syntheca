"""Reporting module exporting convenience helpers for data export.

This package exposes small helpers for writing outputs such as Parquet and
Excel files used by the pipeline and run scripts.  It also defines the
stable output groups and parity validation utilities.
"""

from .export import write_formatted_excel as write_formatted_excel
from .export import write_parquet as write_parquet
from .output_groups import ALL_GROUPS as ALL_GROUPS
from .output_groups import GROUP_REGISTRY as GROUP_REGISTRY
from .output_groups import OutputGroupName as OutputGroupName
