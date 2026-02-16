from __future__ import annotations
import polars as pl


# Unified schema of the resulting table, common to all vendors, so they can be easily merged
UNIFIED_SCHEMA = [
    pl.col("VendorID").cast(pl.Int8, strict=False).alias("VendorID"), # 0 taxi, green: 1, uber:2, lyft: 3, other:drop

    pl.col("pickup_datetime").cast(pl.Datetime("us"), strict=False).alias("pickup_datetime"),
    pl.col("dropoff_datetime").cast(pl.Datetime("us"), strict=False).alias("dropoff_datetime"),

    pl.col("trip_distance").cast(pl.Float32, strict=False).alias("trip_distance"),

    pl.col("PULocationID").cast(pl.Int16, strict=False).alias("PULocationID"),
    pl.col("DOLocationID").cast(pl.Int16, strict=False).alias("DOLocationID"),

    pl.col("payment_type").cast(pl.String, strict=False).alias("payment_type"),

    pl.col("fare_amount").cast(pl.Float32, strict=False).alias("fare_amount"),
    pl.col("tip_amount").cast(pl.Float32, strict=False).alias("tip_amount"),
    pl.col("tolls_amount").cast(pl.Float32, strict=False).alias("tolls_amount"),

    pl.col("total_amount").cast(pl.Float32, strict=False).alias("total_amount")
]


def _real_col(name, columns):
    if name in columns:
        return pl.col(name).cast(pl.Float64, strict=False)
    return pl.lit(0.0)


def _normalize_payment_type_label(expr: pl.Expr) -> pl.Expr:
    s2 = (
        expr.cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(",", "")
            .str.to_lowercase()
    )

    mapping = {
        # numérico
        "0": "flex_fare_tip",
        "1": "credit_card",
        "2": "cash",
        "3": "no_charge",
        "4": "dispute",
        "5": "unknown",
        "6": "voided", 
        # sinónimos texto
        "credit": "credit_card", "cre": "credit_card", "crd": "credit_card",
        "cash": "cash", "cas": "cash", "csh": "cash",
        "no charge": "no_charge", "no": "no_charge",
        "dispute": "dispute", "dis": "dispute",
    }
    return s2.replace_strict(mapping, default="cash")


# ---------- 1) Yellow ----------
def _build_yellow_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the yellow taxi data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    return {
        'create':[
            pl.lit(0).alias("VendorID"),
            _normalize_payment_type_label(pl.col("payment_type")).alias("payment_type")
        ],
        'rename' : {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
        },
    }


# ---------- 2) Green ----------
def _build_green_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the green taxi data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    return {
        'create': [
            pl.lit(1).alias("VendorID"),
            _normalize_payment_type_label(pl.col("payment_type")).alias("payment_type"),
        ],
        'rename': {
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
        },
    }

    
# ---------- 3) For-Hire High-Volume Vehicles ----------
def _build_fhvhv_params(lf: pl.LazyFrame) -> dict:
    """
    Obtains the transformation params for the fhvhv data tables

    Args:
        lf  (pl.LazyFrame): The polars' Lazy Frame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    cols = set(lf.columns)

    return {
        'create': [
            (
                _real_col("base_passenger_fare", cols) +
                _real_col("tolls", cols) +
                _real_col("bcf", cols) +
                _real_col("sales_tax", cols) +
                _real_col("congestion_surcharge", cols) +
                _real_col("cbd_congestion_fee", cols)
            ).alias("total_amount"),
            pl.col("hvfhs_license_num").replace(
                {'HV0002':'4', 'HV0003':'2', 'HV0004':'4', 'HV0005':'3'},
                default=None
            ).cast(pl.Int32, strict=False).alias("VendorID"),
            pl.lit("APP").alias("payment_type")
        ],
        'rename': {
            "trip_miles": "trip_distance",
            "base_passenger_fare": "fare_amount",
            "tolls": "tolls_amount",
            "tips": "tip_amount",
        },
        'apply': [
            lambda x: x.filter(pl.col("VendorID") < 4)
        ]
    }


def normalize_to_target_schema(lf: pl.LazyFrame, vendor: str) -> pl.LazyFrame:
    """
    Apply column transformations to the data

    Args:
        lf          (pl.LazyFrame): Data as polars' lazy frame
        vendor      (str):          Vendor type

    Returns:
        out         (pl.LazyFrame): Returns the new transformed data
    """

    match vendor:
        case "yellow":
            params = _build_yellow_params(lf)

        case "green":
            params = _build_green_params(lf)

        case "fhvhv":
            params = _build_fhvhv_params(lf)

        case _:
            params = {"create": [], "rename": {}}

    lf = (lf.with_columns(params["create"]).rename(params["rename"], strict=False))

    if "apply" in params:
        for tr_fun in params["apply"]:
            lf = tr_fun(lf)
    
    return lf.select(UNIFIED_SCHEMA)
