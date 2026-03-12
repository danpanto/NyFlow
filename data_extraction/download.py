import polars as pl
from pathlib import Path


def get_lazy_frame(date: str, vendor: str) -> tuple:
    """
    Retrieve trip data from a specfic year, month and vendor

    Args:
        date        (str):      Date represented in the format "yyyy-MM"
        vendor      (str):      Vendor type

    Returns:
        out         (tuple):    Returns (int, Any) if error, (polars.LazyFrame, file_url) if success
    """

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
        if magic_bytes != b"PAR1":  # Check if the file is really a parquet, more reliable than content-type or extension
            return (-2, f"Invalid file type: {magic_bytes}")

        file_data = magic_bytes + parquet_response.raw.read()

    return (pl.read_parquet(io.BytesIO(file_data)).lazy(), file_url)


def save_lazy_frame(lf: pl.LazyFrame, year: int, month: str, vendor: str) -> Path:
    """
    Save data to a local file on disk

    Args:
        lf          (pl.LazyFrame): Data as polars' lazy frame
        year        (int):          Year when data was collected
        month       (str):          Month when data was collected
        vendor      (str):          Vendor type

    Returns:
        out         (Path):         Path to the locally saved file
    """

    from os import environ

    data_dir = environ["PD2_DATA_DIR"]
    
    month_dir = Path(data_dir, str(year), month.lstrip('0'))
    month_dir.mkdir(parents=True, exist_ok=True)

    filepath = Path(month_dir, f"{vendor}.parquet")

    lf.sink_parquet(filepath)

    return filepath
