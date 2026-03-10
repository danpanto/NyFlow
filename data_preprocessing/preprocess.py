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

    file_path = Path(merge_path, f"{datetime.now().strftime("%y%m%d_%H%M%S")}_merged.parquet")
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

    file_path = f"merged/{datetime.now().strftime("%y%m%d_%H%M%S")}_merged.parquet"
    
    df = client.read_parquet(files, mergeSchema="true")
    client.write_parquet(df, file_path)

    return file_path
