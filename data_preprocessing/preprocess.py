from concurrent.futures import Executor
from numpy import left_shift
import polars as pl
from pathlib import Path
from minio_utils import MinioSparkClient


def transform_columns(lf: pl.LazyFrame, vendor: str) -> pl.LazyFrame:
    """
    Apply column transformations to the data

    Args:
        lf          (pl.LazyFrame): Data as polars' lazy frame
        vendor      (str):          Vendor type

    Returns:
        out         (pl.LazyFrame): Returns the new transformed data
    """

    from data_preprocessing.field_tranformations import (
        build_yellow_params,
        build_green_params,
        build_fhvhv_params,
        UNIFIED_SCHEMA
    )


    match vendor:
        case "yellow":
            params = build_yellow_params(lf)

        case "green":
            params = build_green_params(lf)

        case "fhvhv":
            params = build_fhvhv_params(lf)

        case _:
            params = {"rename": {}, "create": []}

    # Rename columns
    lf = lf.rename(params.get("rename", {}), strict=False)

    # Create new columns
    if params.get("create"):
        lf = lf.with_columns(params["create"])

    # Apply filters
    if "apply" in params:
        for tr_fun in params["apply"]:
            lf = tr_fun(lf)
    
    return lf.select(UNIFIED_SCHEMA)


def remove_outliers_local(filepaths: set[str]):
    config = {
        "fare_amount":  (0, 10000),
        "tip_amount":   (0, 10000),
        "tolls_amount": (0, 5000),
        "total_amount": (0, 30000)
    }
    clean_paths = set()

    data_dir = Path.cwd() / "data"
    clean_dir = data_dir / "clean"
    clean_dir.mkdir(exist_ok=True, parents=True)

    for file in filepaths:
        lf_final = pl.scan_parquet(file).with_columns([
            pl.col("trip_distance").mul(1609.34).clip(0, 200 * 1609.34).cast(pl.Int32),
            *[pl.col(col).clip(low, high).cast(pl.Int16) for col, (low, high) in config.items()]
        ])

        p = Path(file)
        final_path = clean_dir / f"{'_'.join(p.relative_to(data_dir).parts[:-1])}_{p.name}"

        lf_final.sink_parquet(final_path)
        clean_paths.add(str(final_path))

    return clean_paths


def remove_outliers_minio(filepaths: set[str], client: MinioSparkClient):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, ShortType

    config = {
        "fare_amount":  (0, 10000),
        "tip_amount":   (0, 10000),
        "tolls_amount": (0, 5000),
        "total_amount": (0, 30000)
    }
    clean_paths = set()

    client.connect()

    client.mkdir("clean", exist_ok=True)

    for file in filepaths:
        df_final = client.read_parquet(str(file)).withColumn(
            "trip_distance",
            F.least(
                F.greatest(F.col("trip_distance") * 1609.34, F.lit(0)),
                F.lit(200 * 1609.34)
            ).cast(IntegerType())
        )

        for col_name, (low, high) in config.items():
            df_final = df_final.withColumn(
                col_name,
                F.least(F.greatest(F.col(col_name), F.lit(low)), F.lit(high)).cast(ShortType())
            )

        p = Path(file)
        final_path = f"clean/{p.stem}_clean{p.suffix}"
        client.write_parquet(df_final, final_path)
        clean_paths.add(final_path)

    return clean_paths


def merge_files_local(files: set[str]):
    from datetime import datetime

    data_path = Path.cwd() / "data"
    merge_path = data_path / "merged"
    merge_path.mkdir(exist_ok=True)

    file_path = Path(merge_path, f"{datetime.now().strftime("%Y%m%d_%H%M%S")}_merged.parquet")
    if file_path.exists():
        file_path.unlink()
    
    pl.concat(
        [pl.scan_parquet(f) for f in files],
        how="diagonal",
        rechunk=False
    ).sink_parquet(file_path)

    return str(file_path)


def merge_files_minio(files: set[str], client: MinioSparkClient):
    from datetime import datetime

    client.mkdir("merged", exist_ok=True)
    client.connect()

    file_path = f"merged/{datetime.now().strftime("%Y%m%d_%H%M%S")}_merged.parquet"
    
    df = client.read_parquet(files, mergeSchema="true")
    client.write_parquet(df, file_path)

    return file_path


def prepare_data_local(file: str):
    from datetime import datetime
    from math import pi
    PI2 = 2 * pi

    data_path = Path.cwd() / "data"
    agg_path = data_path / "prepared_for_model"
    agg_path.mkdir(exist_ok=True)

    try:
        lf_cent = pl.scan_parquet(data_path / "map_centroids.parquet")
    except:
        raise Exception("No file was found containing zone centroids data")

    pl.scan_parquet(file).sort(
        "pickup_datetime"
    ).group_by_dynamic(
        "pickup_datetime",
        every="1h",
        group_by=["VendorID", "PULocationID"]
    ).agg(
        pl.len().cast(pl.Int32).alias("demand"),
        pl.col("trip_distance").mean().cast(pl.Float32).alias("avg_distance"),
        pl.col("total_amount").mean().cast(pl.Float32).alias("avg_amount")
    ).select(
        "VendorID",
        "PULocationID",
        pl.col("pickup_datetime").alias("timestamp"),
        "demand",
        "avg_distance",
        "avg_amount"
    ).filter(
        ~(pl.col("PULocationID").is_in([264, 265]))
    ).join(
        lf_cent,
        left_on="PULocationID",
        right_on="locationid",
        how="left"
    ).with_columns([
        pl.col("Latitude").cast(pl.Float32).alias("Latitude"),
        pl.col("Longitude").cast(pl.Float32).alias("Longitude"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.weekday().alias("dow")
    ]).with_columns([
        (pl.col("hour") * (PI2 / 24)).sin().cast(pl.Float32).alias("hour_sin"),
        (pl.col("hour") * (PI2 / 24)).cos().cast(pl.Float32).alias("hour_cos"),
        (pl.col("dow") * (PI2 / 7)).sin().cast(pl.Float32).alias("dow_sin"),
        (pl.col("dow") * (PI2 / 7)).cos().cast(pl.Float32).alias("dow_cos"),
    ]).sink_parquet(Path(agg_path, f"{datetime.now().strftime("%Y%m%d_%H%M%S")}_agg.parquet"))


def prepare_data_minio(file: str, client: MinioSparkClient):
    from datetime import datetime
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType, IntegerType
    from math import pi

    client.mkdir("prepared_for_model", exist_ok=True)
    client.connect()

    try:
        df_cent = client.read_parquet("map_centroids.parquet")
    except:
        raise Exception("No file was found containing zone centroids data")

    PI2 = 2 * pi

    df_agg = client.read_parquet(file).groupBy(
    "VendorID", 
    "PULocationID", 
        F.window("pickup_datetime", "1 hour").alias("window")
    ).agg(
        F.count("*").cast(IntegerType()).alias("demand"),
        F.avg("trip_distance").cast(FloatType()).alias("avg_distance"),
        F.avg("total_amount").cast(FloatType()).alias("avg_amount")
    ).select(
        "VendorID", 
        "PULocationID", 
        F.col("window.start").alias("timestamp"),
        "demand", "avg_distance", "avg_amount"
    ).filter(
        ~(F.col("PULocationID").isin([264, 265]))
    )

    df_final = df_agg.join(
        other=df_cent,
        on=df_agg.PULocationID == df_cent.locationid, 
        how="left"
    ).withColumn(
        "Latitude",
        F.col("Latitude").cast(FloatType())
    ).withColumn(
        "Longitude",
        F.col("Longitude").cast(FloatType())
    ).drop("locationid").withColumn(
        "hour",
        F.hour("timestamp")
    ).withColumn(
        "dow",
        F.dayofweek("timestamp")
    ).withColumn(
        "hour_sin",
        F.sin(F.col("hour") * (PI2 / 24))
    ).withColumn(
        "hour_cos",
        F.cos(F.col("hour") * (PI2 / 24))
    ).withColumn(
        "dow_sin",
        F.sin(F.col("dow") * (PI2 / 7))
    ).withColumn(
        "dow_cos",
        F.cos(F.col("dow") * (PI2 / 7))
    ).dropna()

    client.write_parquet(df_final, f"prepared_for_model/{datetime.now().strftime("%Y%m%d_%H%M%S")}_agg.parquet")
