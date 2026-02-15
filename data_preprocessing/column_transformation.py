from pyspark.sql import DataFrame, functions as func
from pyspark.sql.types import (
    ByteType,
    ShortType,
    IntegerType,
    FloatType,
    DoubleType,
    StringType,
    TimestampType
)


# Unified schema of the resulting table, common to all vendors, so they can be easily merged
UNIFIED_SCHEMA = [
    func.col("VendorID").cast(ByteType()).alias("VendorID"), # 0: yellow, green: 1, uber:2, lyft: 3, default: drop

    func.col("pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
    func.col("dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),

    func.col("trip_distance").cast(FloatType()).alias("trip_distance"),

    func.col("PULocationID").cast(ShortType()).alias("PULocationID"),
    func.col("DOLocationID").cast(ShortType()).alias("DOLocationID"),

    func.col("payment_type").cast(StringType()).alias("payment_type"),

    func.col("fare_amount").cast(FloatType()).alias("fare_amount"),
    func.col("tip_amount").cast(FloatType()).alias("tip_amount"),
    func.col("tolls_amount").cast(FloatType()).alias("tolls_amount"),
    func.col("total_amount").cast(FloatType()).alias("total_amount"),

    func.col("airport_fee").cast(FloatType()).alias("airport_fee"),
]


def _real_col(name, columns):
    if name in columns:
        return func.col(name).cast(DoubleType())
    return func.lit(0.0)


def _normalize_payment_type_label(col_name: str) -> func.Column:
    s2 = func.lower(  # Conver to lowercase
        func.regexp_replace(  # Remove commas
            func.trim(func.col(col_name).cast(StringType())),  # strip
            ",",
            ""
        )
    )

    mapping = [
        # numérico
        ("0", "flex_fare_tip"),
        ("1", "credit_card"),
        ("2", "cash"),
        ("3", "no_charge"),
        ("4", "dispute"),
        ("5", "unknown"),
        ("6", "voided"), 
        # sinónimos texto
        ("credit", "credit_card"), ("cre", "credit_card"), ("crd", "credit_card"),
        ("cash", "cash"), ("cas", "cash"), ("csh", "cash"),
        ("no charge", "no_charge"), ("no", "no_charge"),
        ("dispute", "dispute"), ("dis", "dispute"),
    ]

    # Flatten into 1-D array
    map_expr = func.create_map([func.lit(item) for pair in mapping for item in pair])

    return map_expr[s2]


# ---------- 1) Yellow ----------
def _build_yellow_params(df: DataFrame) -> dict:
    """
    Obtains the transformation params for the yellow taxi data tables

    Args:
        df  (DataFrame):    The spark DataFrame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    return {
        'create': {
            "VendorID": func.lit(0),
            "payment_type": _normalize_payment_type_label("payment_type")
        },
        'rename' : {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "Airport_fee": "airport_fee"
        }
    }


# ---------- 2) Green ----------
def _build_green_params(df: DataFrame) -> dict:
    """
    Obtains the transformation params for the green taxi data tables

    Args:
        df  (DataFrame):    The spark DataFrame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    return {
        'create': {
            "VendorID": func.lit(1),
            "payment_type": _normalize_payment_type_label("payment_type"),
            "airport_fee": func.lit(0.0)
        },
        'rename': {
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
        }
    }


# ---------- 3) For-Hire High-Volume Vehicles ----------
def _build_fhvhv_params(df: DataFrame) -> dict:
    """
    Obtains the transformation params for the fhvhv data tables

    Args:
        df  (DataFrame):    The spark DataFrame containing the data

    Returns:
        out (dict):         The params for the transformation
    """

    cols = df.columns
    vendor_mapping = func.create_map([
        func.lit(item)
        for pair in [('HV0002', '4'), ('HV0003', '2'), ('HV0004', '4'), ('HV0005', '3')]
        for item in pair
    ])

    total_amount_expr = (
        _real_col("base_passenger_fare", cols) +
        _real_col("tolls", cols) +
        _real_col("bcf", cols) +
        _real_col("sales_tax", cols) +
        _real_col("congestion_surcharge", cols) +
        _real_col("airport_fee", cols) +
        _real_col("cbd_congestion_fee", cols)
    )

    return {
        'create': {
            "total_amount": total_amount_expr,
            "VendorID": vendor_mapping[func.col("hvfhs_license_num")].cast(IntegerType()),
            "payment_type": func.lit("APP")
        },
        'rename': {
            "trip_miles": "trip_distance",
            "base_passenger_fare": "fare_amount",
            "tolls": "tolls_amount",
            "tips": "tip_amount",
        },
        'filter': [
            func.col("VendorID") < 4,
        ]
    }


def normalize_to_target_schema(df: DataFrame, vendor: str) -> DataFrame:
    """
    Apply column transformations to the data

    Args:
        df      (DataFrame):    The spark DataFrame containing the data
        vendor  (str):          Vendor type

    Returns:
        out     (pl.LazyFrame): Returns the new transformed data
    """

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
