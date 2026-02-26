import polars as pl
from pathlib import Path


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


def merge_lazy_frames(method: str, remove_files: bool = False) -> list[Path] | None:
    """
    Merge multiple data scattered through various local files into one (or more, depending on criteria) file

    Args:
        method          (str):                      How to merge data (by year, month, ...)
        remove_files    (bool, default = False):    Whether to remove original files after merge

    Returns:
        out             (list[Path] | None):        List of the new local files (or None if error)
    """

    data_path = Path.cwd() / "data"
    merge_path = data_path / "merged"
    merge_path.mkdir(exist_ok=True)

    files: list[Path] = [f for f in data_path.rglob("*.parquet") if f.parent != data_path]

    if not files:
        return None

    ret_paths: list[Path] = []

    if method == "Single file":
        lfs = [pl.scan_parquet(f) for f in files]
        file_path = Path(merge_path, "all_merged.parquet")
        if file_path.exists():
            file_path.unlink()
        pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(file_path)
        ret_paths.append(file_path)

    elif method == "By vendor":
        vendor_groups = {}

        # Get all files separated by vendor
        for f in files:
            vid = f.stem
            vendor_groups.setdefault(vid, []).append(f)

        # Process each vendor group individually
        for vid, grouped_files in vendor_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            file_path = Path(merge_path, f"{vid}_merged.parquet")
            if file_path.exists():
                file_path.unlink()
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(file_path)
            ret_paths.append(file_path)
            
    elif method == "By month":
        month_groups = {}

        # Get all files separated by vendor
        for f in files:
            month = f.parent.name
            month_groups.setdefault(month, []).append(f)

        # Process each month group individually
        for month, grouped_files in month_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            file_path = Path(merge_path, f"{month}_merged.parquet")
            if file_path.exists():
                file_path.unlink()
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(file_path)
            ret_paths.append(file_path)

    elif method == "By year":
        year_groups = {}

        # Get all files separated by vendor
        for f in files:
            year = f.parent.parent.name
            year_groups.setdefault(year, []).append(f)

        # Process each year group individually
        for year, grouped_files in year_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            file_path = Path(merge_path, f"{year}_merged.parquet")
            if file_path.exists():
                file_path.unlink()
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(file_path)
            ret_paths.append(file_path)

    else: return None

    # Handle original files' deletion
    if remove_files:
        for f in files:
            f.unlink()

        for p in sorted(data_path.glob("**/*"), reverse=True):  # Remove child directories before parents
            if p.is_dir() and not any(p.iterdir()):
                p.rmdir()

    return ret_paths


def remove_outliers(
    filepath,
    messenger,
    outliers_cols: list = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"],
    group_col: str = "PULocationID",
    k = 3.0
):
    lf = pl.scan_parquet(filepath)

    transformation_exprs = []

    for col in outliers_cols:
        q1 = pl.col(col).quantile(0.25).over(group_col)
        q3 = pl.col(col).quantile(0.75).over(group_col)
        med = pl.col(col).median().over(group_col)

        iqr = q3 - q1

        expr = (
            pl.when(
                (pl.col(col) < (q1 - k * iqr)) |
                (pl.col(col) > (q3 + k * iqr))
            )
            .then(med)
            .otherwise(pl.col(col))
            .alias(col)
        )

        transformation_exprs.append(expr)

    messenger("Transforming outliers...")

    lf_final = lf.with_columns(transformation_exprs)

    messenger("Saving to disk...")

    lf_final.sink_parquet(Path(filepath.parent, f"{filepath.stem}_cleaned.parquet"))
