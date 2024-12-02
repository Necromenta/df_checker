import pytest
import pandas as pd
from src.DataFrameChangeTracker import DataFrameChangeTracker


@pytest.fixture
def sample_df1():
    return pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )


@pytest.fixture
def sample_df2():
    return pd.DataFrame(
        {
            "id": [1, 2, 4],  # Changed 3 to 4
            "name": ["Alice", "Bob", "David"],  # Changed Charlie to David
            "salary": [50000, 60000, 70000],  # New column replacing 'age'
        }
    )


def test_decorator_usage():
    @DataFrameChangeTracker.track_dataframe_changes
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        df["salary"] = df["age"] * 1000
        df = df.drop("age", axis=1)
        return df

    input_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    result = transform_data(input_df)
    assert "salary" in result.columns
    assert "age" not in result.columns


def test_direct_comparison():
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    df2 = pd.DataFrame({"id": [1, 2, 4], "value": [10, 25, 35]})

    changes = DataFrameChangeTracker.analyze_dataframe_changes(df1, df2)
    assert changes  # The actual structure depends on your implementation


def test_schema_changes():
    df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

    df2 = pd.DataFrame(
        {
            "id": [1, 2],
            "value": ["10", "20"],  # Changed type from int to string
        }
    )

    changes = DataFrameChangeTracker.analyze_dataframe_changes(df1, df2)
    assert changes  # Should detect type change


def test_row_changes():
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    df2 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

    changes = DataFrameChangeTracker.analyze_dataframe_changes(df1, df2)
    assert changes  # Should detect row deletion


def test_column_changes():
    df1 = pd.DataFrame({"id": [1, 2], "old_col": [10, 20]})

    df2 = pd.DataFrame({"id": [1, 2], "new_col": [30, 40]})

    changes = DataFrameChangeTracker.analyze_dataframe_changes(df1, df2)
    assert changes  # Should detect column changes
