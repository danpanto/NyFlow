import polars as pl


month_map = {
    "January":      "01",
    "February":     "02",
    "March":        "03",
    "April":        "04",
    "May":          "05",
    "June":         "06",
    "July":         "07",
    "August":       "08",
    "September":    "09",
    "October":      "10",
    "November":     "11",
    "December":     "12",
}


def get_lazy_frame(year: int, month: str, vendor: str) -> pl.LazyFrame | tuple:
    import requests as rq
    import io


    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": url,
        "Accept": "application/octet-stream" 
    }

    session = rq.Session()
    session.headers.update(headers)

    session.get(url)
    file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{vendor}_tripdata_{year}-{month_map[month]}.parquet"
    parquet_response = session.get(file_url, stream=True)

    if parquet_response.status_code // 100 != 2:
        return (-1, parquet_response.status_code)

    content_type = parquet_response.headers.get('Content-Type', '')

    if content_type != "binary/octet-stream":
        return (-2, content_type)

    return pl.read_parquet(io.BytesIO(parquet_response.content)).lazy()


def apply_transformations(lf: pl.LazyFrame, vendor: str, which: str) -> pl.LazyFrame:
    from data_preprocessing.field_tranformations import normalize_to_target_schema, yellow_params, green_params, uber_params

    
    params = {}

    match vendor:
        case "yellow":
            params = yellow_params

        case "green":
            params = green_params

        case "fhvhv":
            params = uber_params

    if which in ("Add/Del columns", "All"):
        lf = normalize_to_target_schema(
            lf,
            coalesce=params.get("coalesce", []),
            rename=params.get("rename", {}),
            required_schema=params.get("required_schema", [])
        )

    if which in ("Outliers", "All"):
        # Call outlier removal processes
        pass

    return lf


def save_lazy_frame(lf: pl.LazyFrame, year: int, month: str, vendor: str) -> None:
    from pathlib import Path


    month_path = Path.cwd() / "data" / str(year) / month
    month_path.mkdir(parents=True, exist_ok=True)

    lf.sink_parquet(Path(month_path, f"{vendor}.parquet"))


def merge_lazy_frames(method: str, remove_files: bool = False):
    from pathlib import Path
    import requests as rq


    data_path = Path.cwd() / "data"

    files: list[Path] = [f for f in data_path.rglob("*.parquet") if f.parent != data_path]

    if not files:
        return

    if method == "Single file":
        lfs = [pl.scan_parquet(f) for f in files]
        pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(Path(data_path, "all_merged.parquet"))

    elif method == "By vendor":
        vendor_groups = {}

        # Get all files separated by vendor
        for f in files:
            vid = f.stem
            vendor_groups.setdefault(vid, []).append(f)

        # Process each vendor group individually
        for vid, grouped_files in vendor_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(Path(data_path, f"{vid}_merged.parquet"))
            
    elif method == "By month":
        month_groups = {}

        # Get all files separated by vendor
        for f in files:
            month = f.parent.name
            month_groups.setdefault(month, []).append(f)

        # Process each month group individually
        for month, grouped_files in month_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(Path(data_path, f"{month}_merged.parquet"))

    elif method == "By year":
        year_groups = {}

        # Get all files separated by vendor
        for f in files:
            year = f.parent.parent.name
            year_groups.setdefault(year, []).append(f)

        # Process each year group individually
        for year, grouped_files in year_groups.items():
            lfs = [pl.scan_parquet(f) for f in grouped_files]
            pl.concat(lfs, how="diagonal", rechunk=False).sink_parquet(Path(data_path, f"{year}_merged.parquet"))

    else: return

    # Handle original files' deletion
    if not remove_files:
        return

    for f in files:
        f.unlink()

    for p in sorted(data_path.glob("**/*"), reverse=True):  # Remove child directories before parents
        if p.is_dir() and not any(p.iterdir()):
            p.rmdir()
    


if __name__ == '__main__':
    lf = get_lazy_frame(year=2025, month="January", vendor="yellow")
    transf_lf = apply_transformations(lf, "yellow", "Add/Del columns")
    save_lazy_frame(transf_lf, year=2025, month="January", vendor="yellow")
