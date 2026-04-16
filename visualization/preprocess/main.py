import argparse
from datetime import datetime
import polars as pl
from rich.console import Console


def main():
    console = Console()

    parser = argparse.ArgumentParser(
        description="Pure lazy pipeline for massive datasets."
    )
    parser.add_argument("in_path", type=str)
    parser.add_argument("out_path", type=str)
    args = parser.parse_args()

    try:
        console.print(f"[bold cyan]Mapping data from:[/bold cyan] {args.in_path}")

        # 1. Map the file lazily
        lf = pl.scan_parquet(args.in_path)

        # 2. Define columns
        datetimeCols = ["pickup_datetime", "dropoff_datetime"]
        i32Cols = ["PULocationID", "DOLocationID"]
        groupbyCols = ["pickup_datetime", "VendorID", "PULocationID"]
        aggregationCols = [
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "duration",
            "tolls_amount",
            "total_amount",
        ]

        # 3. Build the transformation and aggregation graph
        lf_agg = (
            lf.filter(
                pl.col("pickup_datetime").is_between(
                    datetime(2021, 1, 1), datetime(2025, 12, 31, 23, 59, 59)
                )
            )
            .with_columns(
                (pl.col("dropoff_datetime") - pl.col("pickup_datetime"))
                .dt.total_minutes(fractional=True)
                .alias("duration"),
                pl.col(datetimeCols).dt.truncate("1h"),
                pl.col(i32Cols).cast(pl.Int16),
            )
            .filter(
                pl.col("duration") > 0,
            )
            .group_by(groupbyCols)
            .agg(
                pl.len().alias("count"),
                *[pl.col(name).sum().alias(name) for name in aggregationCols],
            )
            .sort(groupbyCols)
        )

        with console.status(
            f"[bold cyan]Streaming aggregated data to:[/bold cyan] {args.out_path}",
            spinner="dots",
        ):
            lf_agg.sink_parquet(args.out_path)

        console.print("\n[bold green]✔ Pipeline completed safely![/bold green]")

    except Exception as e:
        console.print(f"[bold red]An error occurred:[/bold red] {e}")


if __name__ == "__main__":
    main()
