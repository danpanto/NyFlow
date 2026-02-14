"""
    This script contains methods to optimize the disk and RAM
    usage of dataframes through preprocessing
"""

from pathlib import Path
from typing import Callable, TypeVar
import polars as pl
import io

# Type definitions
TransformationFunc = Callable[[str], pl.Expr]
AnyDataframe = TypeVar("AnyDataframe", pl.DataFrame, pl.LazyFrame)

# This schema contains the data conversions required
# of the dataframes for memory optimization purposes
nyc_schema_optimization_labels: dict[str, list[str]] = {
    "int8": [
        "VendorID", "RatecodeID", "passenger_count", "payment_type", 
        "trip_type"
    ],
    "int16": [
        "PULocationID", "DOLocationID"
    ],
    "int32": [
        "trip_time",
    ],
    "float32": [
        "trip_distance", "trip_miles", "fare_amount", "extra", "mta_tax", "tip_amount", 
        "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", 
        "congestion_surcharge", "cbd_congestion_fee",
        "base_passenger_fare", "tolls", "bcf", "sales_tax", 
        "Airport_fee", "airport_fee", "tips", "driver_pay"
    ],
    "bool_yn": [
        "store_and_fwd_flag", "shared_request_flag", "shared_match_flag", 
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag"
    ],
    "datetime": [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "lpep_pickup_datetime", "lpep_dropoff_datetime", 
        "request_datetime", "on_scene_datetime", 
        "pickup_datetime", "dropoff_datetime"
    ],
    "categorical": [
        "hvfhs_license_num", "dispatching_base_num", "originating_base_num"
    ]
}

nyc_schema_optimization_transformations: dict[str, TransformationFunc] = {
    "int8": lambda col: pl.col(col).cast(pl.Int8),
    "int16": lambda col: pl.col(col).cast(pl.Int16),
    "int32": lambda col: pl.col(col).cast(pl.Int32),
    "float32": lambda col: pl.col(col).cast(pl.Float32),
    "money": lambda col: (pl.col(col) * 100).clip(-(2**31), 2**31 - 1).cast(pl.Int32),
    "bool_yn": lambda col: (pl.col(col) == "Y").alias(col),
    "datetime": lambda col: pl.col(col).dt.truncate("1s").cast(pl.Datetime("ms")),
    "categorical": lambda col: pl.col(col).cast(pl.Categorical)
}

def optimize_dataframe(
    df: AnyDataframe,
    sort_by_column: str | None = None,
    labels: dict[str, list[str]] = nyc_schema_optimization_labels,
    transformations: dict[str, TransformationFunc] = nyc_schema_optimization_transformations,
) -> AnyDataframe:
    """
    Optimizes a Polars DataFrame/LazyFrame by applying type casting and transformations.
    It also handles optional sorting to maximize Parquet's RLE (Run-Length Encoding) efficiency.

    Args:
        df: The input Polars DataFrame or LazyFrame to optimize.
        sort_by_column: The name of the column to sort the data by. Highly recommended 
            to use a high-cardinality time-based column for better compression.
        labels: A dictionary mapping transformation category keys to lists of column names.
        transformations: A dictionary mapping category keys to lambda functions that 
            generate Polars expressions.
    Returns:
        The optimized DataFrame or LazyFrame with updated schema and sorted rows.
    """

    expressions: list[pl.Expr] = []
    transformed_columns: set[str] = set()
    df_columns: list[str] = df.columns

    for category, cols in labels.items():
        active_cols: list[str] = [c for c in cols if c in df_columns]

        if len(active_cols) == 0:
            continue

        transform_fn: TransformationFunc | None= transformations.get(category)

        if transform_fn is None:
            print(f"ERROR: Couldn't get transformation function for category '{category}'")
            continue

        for col in active_cols:
            if col in transformed_columns:
                print(f"ERROR: '{col}' column appears transformed twice.")
            else:
                transformed_columns.add(col)
                expressions.append(transform_fn(col))

    for remaining_cols in set(df_columns).difference(transformed_columns):
        print(f"WARNING: '{remaining_cols}' column hasn't been optimized.")

    if sort_by_column is None:
        return df.with_columns(expressions)
    elif sort_by_column in df_columns:
        return df.with_columns(expressions).sort(sort_by_column)
    else:
        print(f"ERROR: '{sort_by_column}' is an invalid column to sort by.")
        return df.with_columns(expressions)

def get_sort_column_by_schema(df: AnyDataframe) -> str | None:
    """
    Identifies the correct pickup datetime column by checking available columns.
    
    Args:
        df: The Polars DataFrame or LazyFrame to inspect.
    """
    cols = df.columns
    
    if "tpep_pickup_datetime" in cols:
        return "tpep_pickup_datetime"  # Yellow Taxi
    elif "lpep_pickup_datetime" in cols:
        return "lpep_pickup_datetime"  # Green Taxi
    elif "pickup_datetime" in cols:
        return "pickup_datetime"       # Uber/Lyft (HVFHS) or FHV
    
    return None

def compare_polars_dfs(df_raw: pl.DataFrame, df_opt: pl.DataFrame):
    """
    Compares two Polars DataFrames to show gains from type optimization.

    Args:
        df_raw: The original DataFrame before any type casting or
                optimization has been applied.
        df_opt: The transformed DataFrame after applying optimizations
    """

    # Estimate Memory Usage (RAM)
    mem_raw = df_raw.estimated_size()
    mem_opt = df_opt.estimated_size()

    # Simulate Parquet Storage Size (Compressed)
    buffer_raw = io.BytesIO()
    df_raw.write_parquet(buffer_raw, compression="zstd")
    storage_raw = buffer_raw.getbuffer().nbytes

    buffer_opt = io.BytesIO()
    df_opt.write_parquet(buffer_opt, compression="zstd", compression_level=12)
    storage_opt = buffer_opt.getbuffer().nbytes

    # Helper for formatting
    def to_mb(b: int | float) -> float: return b / (1024 * 1024)

    print(f"{'Metric':<20} | {'Raw DataFrame':<15} | {'Optimized':<15} | {'Improvement'}")
    print("-" * 75)

    # RAM Comparison
    ram_gain = (1 - (mem_opt / mem_raw)) * 100
    print(f"{'RAM Usage':<20} | {to_mb(mem_raw):>11.2f} MB | {to_mb(mem_opt):>11.2f} MB | {ram_gain:>9.1f}%")

    # Storage Comparison
    storage_gain = (1 - (storage_opt / storage_raw)) * 100
    print(f"{'Est. Parquet Size':<20} | {to_mb(storage_raw):>11.2f} MB | {to_mb(storage_opt):>11.2f} MB | {storage_gain:>9.1f}%")

    print("-" * 75)

if __name__ == "__main__":
    print("> YELLOW TAXI:")
    df_raw = pl.read_parquet("data/2025/1/yellow_taxi.parquet")
    df_opt = optimize_dataframe(df_raw, "tpep_pickup_datetime")
    compare_polars_dfs(df_raw, df_opt)

    print("\n> GREEN TAXI:")
    df_raw = pl.read_parquet("data/2025/1/green_taxi.parquet")
    df_opt = optimize_dataframe(df_raw, "lpep_pickup_datetime")
    compare_polars_dfs(df_raw, df_opt)

    print("\n> HVFHV:")
    df_raw = pl.read_parquet("data/2025/1/uber.parquet")
    df_opt = optimize_dataframe(df_raw, "pickup_datetime")
    compare_polars_dfs(df_raw, df_opt)

