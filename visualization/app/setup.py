import sys
import json
import polars as pl
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from rich.console import Console
import geopandas as gpd
from sklearn.neighbors import BallTree
import numpy as np

from .setup_minio import load_minio_client, ensure_files_downloaded

console = Console()

CACHE_DIR = Path("visualization/data")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_FILES = {
    "taxi_zones.geojson": "cityenjoyer/taxi_zones.geojson",
    "aggregation.parquet": "cityenjoyer/visualization_aggregated.parquet",
}

def load_and_validate_file(file_path: Path):
    """
    Validates file existence and structural integrity based on its extension.
    Returns the loaded object (LazyFrame for parquet, dict for JSON).
    """
    if not file_path.exists():
        console.print(f"[bold red][X] Fatal Error: Missing required file {file_path}[/bold red]")
        sys.exit(1)

    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        try:
            lf = pl.scan_parquet(file_path)
            _ = lf.collect_schema() # Force Polars to read headers to catch corruption
            console.print(f"[bold blue][#] Validated and loaded {file_path.name}[/bold blue]")
            return lf
        except Exception as e:
            console.print(f"[bold red][X] Fatal Error: Corrupted parquet file {file_path.name}: {e}[/bold red]")
            sys.exit(1)

    elif suffix in [".json", ".geojson"]:
        try:
            with file_path.open("r") as f:
                data = json.load(f)
            console.print(f"[bold blue][#] Validated and loaded {file_path.name}[/bold blue]")
            return data
        except json.JSONDecodeError:
            console.print(f"[bold red][X] Fatal Error: {file_path.name} is not a valid JSON file.[/bold red]")
            sys.exit(1)
            
    else:
        # Fallback for unknown file types (just check existence)
        console.print(f"[bold yellow][!] Warning: No specific validation rules for {suffix} files ({file_path.name}).[/bold yellow]")
        return file_path

@asynccontextmanager
async def lifespan(app: FastAPI):
    console.print("[bold green]>>> Starting application...[/bold green]")

    # 1. Attempt MinIO Sync (Offline Fallback)
    try:
        app.state.minio_client = load_minio_client()
        await ensure_files_downloaded(app.state.minio_client, "pd2", CACHE_DIR, REQUIRED_FILES)
        console.print("[bold green][+] MinIO sync successful.[/bold green]")
    except Exception as e:
        console.print(f"[bold yellow][!] MinIO connection failed: {e}[/bold yellow]")
        console.print("[bold yellow][!] Falling back to OFFLINE MODE. Relying on local cache.[/bold yellow]")
        app.state.minio_client = None

    # 2. Iterate through REQUIRED_FILES and validate them dynamically
    app.state.files = {}
    for filename in REQUIRED_FILES.keys():
        file_path = CACHE_DIR / filename
        # Store the loaded data in a dictionary on the app state
        app.state.files[filename] = load_and_validate_file(file_path)

    # 3. Assign to easy-to-use variables
    app.state.lf = app.state.files["aggregation.parquet"]
    app.state.taxi_zones = app.state.files["taxi_zones.geojson"]
    app.state.gdf_zones = gpd.GeoDataFrame.from_features(app.state.files["taxi_zones.geojson"]["features"])
    app.state.gdf_zones.set_crs(epsg=4326, inplace=True)


    # 4. Extract IDs and validate specific GeoJSON business logic
    try:
        features = app.state.taxi_zones["features"]
        ids = [int(feature["properties"]["locationid"]) for feature in features]
        app.state.ids = pl.DataFrame({"PULocationID": ids}).lazy()
    except KeyError as e:
        console.print(f"[bold red][X] Fatal Error: Invalid GeoJSON structure. Missing key: {e}[/bold red]")
        sys.exit(1)
    except ValueError:
        console.print("[bold red][X] Fatal Error: A 'locationid' in the GeoJSON could not be converted to an integer.[/bold red]")
        sys.exit(1)

    # Para las rutas
    coords = np.array(list(zip(app.state.gdf_zones.geometry.centroid.y, app.state.gdf_zones.geometry.centroid.x)))
    coords_rad = np.deg2rad(coords)
    app.state.tree = BallTree(coords_rad, metric='haversine')

    yield

    console.print("[bold red]<<< Shutting down application...[/bold red]")
    app.state.files = {}
    app.state.taxi_zones = None
    app.state.minio_client = None

app = FastAPI(lifespan=lifespan)
