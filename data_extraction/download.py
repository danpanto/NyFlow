import duckdb
import polars as pl
from pathlib import Path


def get_lazy_frame(date: str, vendor: str) -> tuple:
    import requests as rq
    import io


    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": url,
        "Accept": "application/octet-stream; application/x-www-form-urlencoded" 
    }

    session = rq.Session()
    session.headers.update(headers)

    session.get(url)
    file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{vendor}_tripdata_{date}.parquet"

    with session.get(file_url, stream=True) as parquet_response:
        if parquet_response.status_code // 100 != 2:
            return (-1, parquet_response.status_code)

        magic_bytes = parquet_response.raw.read(4)
        if magic_bytes != b"PAR1":
            return (-2, f"Invalid Parquet Magic Bytes: {magic_bytes}")

        file_data = magic_bytes + parquet_response.raw.read()

    return (pl.read_parquet(io.BytesIO(file_data)).lazy(), file_url)


def apply_transformations(lf: pl.LazyFrame, vendor: str) -> pl.LazyFrame:
    from data_preprocessing.field_tranformations import (
        normalize_to_target_schema,
        yellow_params,
        green_params,
        build_uber_params
    )
    
    param_builders = {
        "yellow": lambda lf: yellow_params,
        "green": lambda lf: green_params,
        "fhvhv": build_uber_params,
    }

    try:
        params = param_builders[vendor](lf)
    except KeyError:
        raise ValueError(f"Unknown vendor: {vendor}")

    return normalize_to_target_schema(lf, **params)


def save_lazy_frame(lf: pl.LazyFrame, year: int, month: str, vendor: str) -> Path:
    month_path = Path.cwd() / "data" / str(year) / month
    month_path.mkdir(parents=True, exist_ok=True)
    filepath = Path(month_path, f"{vendor}.parquet")

    lf.sink_parquet(filepath)

    return filepath


def merge_lazy_frames(method: str, remove_files: bool = False) -> list[Path] | None:
    from pathlib import Path
    import requests as rq


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
    

def rm_outliers(filepath: Path):
    from data_preprocessing.outliers.outlier_del import remove_outliers

    remove_outliers(
        filepath=filepath,
        outliers_cols=[
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount"
        ]
    )


if __name__ == '__main__':
    lf = get_lazy_frame(year=2025, month="January", vendor="yellow")
    transf_lf = apply_transformations(lf, "yellow", "Add/Del columns")
    save_lazy_frame(transf_lf, year=2025, month="January", vendor="yellow")
