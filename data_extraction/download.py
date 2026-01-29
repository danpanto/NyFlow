import polars as pl


def get_lazy_frame(year: int, month: int, vendor: str) -> pl.LazyFrame | tuple:
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
    file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{vendor}_tripdata_{year}-{str(month):0>2}.parquet"
    parquet_response = session.get(file_url, stream=True)

    if parquet_response.status_code // 100 != 2:
        return (-1, parquet_response.status_code)

    content_type = parquet_response.headers.get('Content-Type', '')

    if content_type != "binary/octet-stream":
        return (-2, content_type)

    return pl.read_parquet(io.BytesIO(parquet_response.content)).lazy()


def apply_transformations(lf: pl.LazyFrame, vendor: str) -> pl.LazyFrame:
    from data_preprocessing.field_tranformations import normalize_to_target_schema, yellow_params, green_params, uber_params

    params = {}

    match vendor:
        case "yellow":
            params = yellow_params

        case "green":
            params = green_params

        case "fhvhv":
            params = uber_params


    return normalize_to_target_schema(
        lf,
        coalesce=params.get("coalesce", []),
        rename=params.get("rename", {}),
        required_schema=params.get("required_schema", [])
    )


def save_lazy_frame(lf: pl.LazyFrame, year: int, month: int, vendor: str) -> None:
    from pathlib import Path


    month_path = Path.cwd() / "data" / str(year) / str(month)
    month_path.mkdir(parents=True, exist_ok=True)

    lf.sink_parquet(Path(month_path, f"{vendor}.parquet"))


if __name__ == '__main__':
    lf = get_lazy_frame(year=2025, month=1, vendor="yellow")
    transf_lf = apply_transformations(lf, "yellow", "Add/Del columns")
    save_lazy_frame(transf_lf, year=2025, month=1, vendor="yellow")
