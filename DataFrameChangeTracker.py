from functools import wraps
from typing import Any, Callable, Dict

import pandas as pd
from tabulate import tabulate


class DataFrameChangeTracker:
    """Tracks changes to DataFrames between function calls."""

    @staticmethod
    def compare_df(df_before: pd.DataFrame, df_after: pd.DataFrame) -> Dict[str, Any]:
        """Compare two dataframes and return a dictionary of changes."""
        changes: dict = {
            "rows": {
                "before": len(df_before),
                "after": len(df_after),
                "difference": len(df_after) - len(df_before),
            },
            "columns": {
                "added": list(set(df_after.columns) - set(df_before.columns)),
                "removed": list(set(df_before.columns) - set(df_after.columns)),
                "modified": [],
            },
            "schema_changes": {},
            "content_changes": set(),
        }

        # Check for schema changes in before-after df
        # This line makes two sets of column names for both dataframes,
        # then the & operator makes it that common_cols will only have
        # values contained in both

        common_cols = set(df_before.columns) & set(df_after.columns)
        for col in common_cols:
            # iterate through all the columns datatypes
            # the IF statement is active if the dtypes are different
            # this to detect any corruption during change
            before_dtype = df_before[col].dtype
            after_dtype = df_after[col].dtype

            if before_dtype != after_dtype:
                changes["schema_changes"][col] = {
                    "before": str(before_dtype),
                    "after": str(after_dtype),
                }

            # Check if content changed in the columns
            # if the columns are different in any iteration it appends them
            if not df_before[col].equals(df_after[col]):
                changes["content_changes"].add(col)
                changes["columns"]["modified"].append(col)

        changes["content_changes"] = list(changes["content_changes"])
        return changes


def track_dataframe_changes(func: Callable) -> Callable:
    """Decorator to track changes to DataFrames."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Find DataFrame in args or kwargs
        df_before = None
        for arg in args:
            if isinstance(arg, pd.DataFrame):
                df_before = arg.copy()
                break

        if df_before is None:
            for arg in kwargs.values():
                if isinstance(arg, pd.DataFrame):
                    df_before = arg.copy()
                    break

        result = func(*args, **kwargs)

        if isinstance(result, pd.DataFrame) and df_before is not None:
            changes = DataFrameChangeTracker.compare_df(df_before, result)

            print(f"\nFunction: {func.__name__}")

            #
            if changes["rows"]["difference"] != 0:
                print(
                    f"Rows: {changes['rows']['before']} → {changes['rows']['after']} ({changes['rows']['difference']:+d})"
                )
            else:
                print("No row changes detected")

            if changes["content_changes"]:
                print(f"Content changes in columns: {changes['content_changes']}")
            # Inside track_dataframe_changes decorator
            for col in changes["content_changes"]:
                # Get the common indices
                common_indices = df_before.index.intersection(result.index)

                # Compare only the common rows
                changed_mask = (
                    df_before.loc[common_indices, col]
                    != result.loc[common_indices, col]
                )

                if changed_mask.any():
                    print(f"\nChanges in {col}:")
                    comparison = pd.DataFrame(
                        {
                            "Before": df_before.loc[changed_mask, col],
                            "After": result.loc[changed_mask, col],
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

        return result

    return wrapper
