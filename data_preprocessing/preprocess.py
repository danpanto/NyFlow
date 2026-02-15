from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as func
from pathlib import Path


def transform_columns(df: DataFrame, vendor: str) -> DataFrame:
    """
    Apply column transformations to the data

    Args:
        df      (DataFrame):    The spark DataFrame containing the data
        vendor  (str):          Vendor type

    Returns:
        out     (pl.LazyFrame): Returns the new transformed data
    """
    from data_preprocessing.column_transformation import (
        _build_yellow_params,
        _build_green_params,
        _build_fhvhv_params,
        UNIFIED_SCHEMA
    )

    match vendor:
        case "yellow":  params = _build_yellow_params(df)
        case "green":   params = _build_green_params(df)
        case "fhvhv":   params = _build_fhvhv_params(df)
        case _:         params = {"create": {}, "rename": {}}

    # Rename columns
    for old_col, new_col in params.get("rename", {}).items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Create/Modify columns
    for col_name, expr in params.get("create", {}).items():
        df = df.withColumn(col_name, expr)

    # Apply filters
    if "filter" in params:
        for cond in params["filter"]:
            df = df.filter(cond)

    # Just in case
    if "airport_fee" not in df.columns:
        df = df.withColumn("airport_fee", func.lit(0.0))
    
    return df.select(*UNIFIED_SCHEMA)
