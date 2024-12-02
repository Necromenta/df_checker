"""
DataFrameChangeTracker.py

This module provides a decorator to track changes to DataFrames between function calls.
"""

from functools import wraps
from typing import Callable, Dict, List, Set
from dataclasses import dataclass, field

import pandas as pd
from tabulate import tabulate


@dataclass
class RowChanges:
    before: int
    after: int
    difference: int = field(init=False)

    def __post_init__(self):
        self.difference = self.after - self.before


@dataclass
class ColumnChanges:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)


@dataclass
class DataFrameChanges:
    rows: RowChanges
    columns: ColumnChanges = field(default_factory=ColumnChanges)
    schema_changes: Dict[str, Dict[str, str]] = field(default_factory=dict)
    content_changes: List[str] = field(default_factory=list)


class DataFrameChangeTracker:
    """Tracks changes to DataFrames between function calls."""

    @staticmethod
    def compare_df(df_before: pd.DataFrame, df_after: pd.DataFrame) -> DataFrameChanges:
        """Compare two dataframes and return a structured object of changes."""
        changes = DataFrameChanges(
            rows=RowChanges(before=len(df_before), after=len(df_after))
        )

        # Column changes
        changes.columns.added = list(set(df_after.columns) - set(df_before.columns))
        changes.columns.removed = list(set(df_before.columns) - set(df_after.columns))

        # Rest of the comparison logic remains similar
        content_changes: Set[str] = set()

        common_cols = set(df_before.columns) & set(df_after.columns)
        for col in common_cols:
            before_dtype = df_before[col].dtype
            after_dtype = df_after[col].dtype

            if before_dtype != after_dtype:
                changes.schema_changes[col] = {
                    "before": str(before_dtype),
                    "after": str(after_dtype),
                }

            if not df_before[col].equals(df_after[col]):
                content_changes.add(col)
                changes.columns.modified.append(col)

        changes.content_changes = list(content_changes)
        return changes

    @staticmethod
    def _find_dataframe_arg(*args, **kwargs) -> pd.DataFrame | None:
        """Find and return the first DataFrame from args or kwargs."""
        # Check positional arguments
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                return arg.copy()

        # Check keyword arguments
        for arg in kwargs.values():
            if isinstance(arg, pd.DataFrame):
                return arg.copy()

        return None

    @staticmethod
    def _print_content_changes(
        df_before: pd.DataFrame, df_after: pd.DataFrame, content_changes: List[str]
    ) -> None:
        """Print detailed comparison of content changes between two DataFrames.

        Args:
            df_before: Original DataFrame
            df_after: Modified DataFrame
            content_changes: List of column names with changes
        """
        for col in content_changes:
            # Get the common indices
            common_indices = df_before.index.intersection(df_after.index)

            # Compare only the common rows
            changed_mask = (
                df_before.loc[common_indices, col] != df_after.loc[common_indices, col]
            )

            if changed_mask.any():
                print(f"\nChanges in {col}:")
                comparison = pd.DataFrame(
                    {
                        "Before": df_before.loc[changed_mask, col],
                        "After": df_after.loc[changed_mask, col],
                    }
                )
                # Add index labels for context
                context_cols = ["name"] if "name" in df_before.columns else []
                if context_cols:
                    for context_col in context_cols:
                        comparison[context_col] = df_before.loc[
                            changed_mask, context_col
                        ]

                print(
                    tabulate(
                        comparison, headers="keys", tablefmt="grid", showindex=False
                    )
                )

    @staticmethod
    def analyze_dataframe_changes(
        df_before: pd.DataFrame,
        df_after: pd.DataFrame,
        function_name: str | None = None,
    ) -> None:
        """Analyze and print changes between two DataFrames.

        Args:
            df_before: Original DataFrame
            df_after: Modified DataFrame
            function_name: Optional name of the function being tracked
        """
        changes = DataFrameChangeTracker.compare_df(df_before, df_after)

        if function_name:
            print(f"\nFunction: {function_name}")

        if changes.rows.difference != 0:
            print(
                f"Rows: {changes.rows.before} → {changes.rows.after} ({changes.rows.difference:+d})"
            )
        else:
            print("No row changes detected")

        if changes.content_changes:
            print(f"Content changes in columns: {changes.content_changes}")
            DataFrameChangeTracker._print_content_changes(
                df_before, df_after, changes.content_changes
            )

    @staticmethod
    def track_dataframe_changes(func: Callable) -> Callable:
        """Decorator to track changes to DataFrames."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            df_before = DataFrameChangeTracker._find_dataframe_arg(*args, **kwargs)

            result = func(*args, **kwargs)

            if isinstance(result, pd.DataFrame) and df_before is not None:
                DataFrameChangeTracker.analyze_dataframe_changes(
                    df_before=df_before, df_after=result, function_name=func.__name__
                )

            return result

        return wrapper
