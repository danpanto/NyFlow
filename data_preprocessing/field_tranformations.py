from __future__ import annotations
import polars as pl


def _coalesce(name, columns):
    return (pl.col(name) if name in columns else pl.lit(0.0)).cast(pl.Float32)


def _to_cents(expr: pl.Expr, dtype):
    return (expr * 100).round(0).cast(dtype)


# Unified schema of the resulting table, common to all vendors, so they can be easily merged
UNIFIED_SCHEMA = [
    pl.col("VendorID").cast(pl.Int8).alias("VendorID"),
    pl.col("pickup_datetime").cast(pl.Datetime("us")).alias("pickup_datetime"),
    pl.col("dropoff_datetime").cast(pl.Datetime("us")).alias("dropoff_datetime"),
    pl.col("trip_distance").cast(pl.Float32).alias("trip_distance"),
    pl.col("PULocationID").cast(pl.Int16).alias("PULocationID"),
    pl.col("DOLocationID").cast(pl.Int16).alias("DOLocationID"),
    pl.col("payment_type").fill_null(5).cast(pl.Int8).alias("payment_type"),
    _to_cents(pl.col("tip_amount"), pl.Int64).alias("tip_amount"),
    _to_cents(pl.col("tolls_amount"), pl.Int64).alias("tolls_amount"),
    _to_cents(pl.col("fare_amount"), pl.Int64).alias("fare_amount"),
    _to_cents(pl.col("total_amount"), pl.Int64).alias("total_amount"),
]


# ---------- 1) Yellow ----------
def build_yellow_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the yellow taxi data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    }

    return {
        "rename": rename_map,
        "create": [pl.lit(0, dtype=pl.Int8).alias("VendorID")],
    }


# ---------- 2) Green ----------
def build_green_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the green taxi data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    rename_map = {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
    }

    return {
        "rename": rename_map,
        "create": [
            pl.lit(1, dtype=pl.Int8).alias("VendorID"),
        ],
    }


# ---------- 3) For-Hire High-Volume Vehicles ----------
def build_fhvhv_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the fhvhv data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    rename_map = {
        "trip_miles": "trip_distance",
        "base_passenger_fare": "fare_amount",
        "tolls": "tolls_amount",
        "tips": "tip_amount",
    }
    cols = set(rename_map.get(c, c) for c in lf.collect_schema().names())

    return {
        "rename": rename_map,
        "create": [
            (
                _coalesce("fare_amount", cols)
                + _coalesce("tolls_amount", cols)
                + _coalesce("bcf", cols)
                + _coalesce("sales_tax", cols)
                + _coalesce("congestion_surcharge", cols)
                + _coalesce("cbd_congestion_fee", cols)
            ).alias("total_amount"),
            pl.col("hvfhs_license_num")
            .replace({"HV0003": 2, "HV0005": 3}, default=4)
            .alias("VendorID"),
            pl.lit(7, dtype=pl.Int8).alias("payment_type"),
        ],
        "apply": [lambda x: x.filter(pl.col("VendorID") < 4)],
    }
