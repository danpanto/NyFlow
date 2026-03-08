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


def merge_lazy_frames(method: str, files: list[Path], remove_files: bool = False) -> list[Path] | None:
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


def remove_outliers(filepath: Path):
    parts = filepath.relative_to(Path.cwd()).parts
    out_path = Path(Path.cwd(), parts[0], "clean", *parts[1:-1])
    out_path.mkdir(exist_ok=True, parents=True)
    
    lf = pl.scan_parquet(filepath)
    config = {
        "fare_amount":  (0, 10000),
        "tip_amount":   (0, 10000),
        "tolls_amount": (0, 5000),
        "total_amount": (0, 30000)
    }

    lf_final = lf.with_columns([
        pl.col("trip_distance").mul(1609.34).clip(0, 200 * 1609.34).cast(pl.Int32),
        *[pl.col(col).clip(low, high).cast(pl.Int16) for col, (low, high) in config.items()]
    ])

    lf_final.sink_parquet(Path(out_path, parts[-1]))
