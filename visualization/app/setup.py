from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
import json

from rich.console import Console
from .setup_minio import load_minio_client, ensure_files_downloaded

console = Console()

data_path = Path("visualization/data")

CACHE_DIR = Path("visualization/data")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_FILES = {
    "taxi_zones.geojson": "cityenjoyer/taxi_zones.geojson",
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    console.print("[bold green]>>> Starting application...[/bold green]")

    try:
        path = (data_path / "taxi_zones.geojson").resolve()
        with path.open("r") as f:
            app.state.taxi_zones = json.load(f)
        console.print(f"[bold blue][#] Loaded taxi zones from {path.name}[/bold blue]")
    except FileNotFoundError:
        console.print("[bold yellow][!] Warning: taxi_zones.geojson not found.[/bold yellow]")
        app.state.taxi_zones = None
    except json.JSONDecodeError:
        console.print("[bold red][!] Error: taxi_zones.geojson is not a valid JSON file.[/bold red]")
        app.state.taxi_zones = None

    app.state.minio_client = load_minio_client()
    await ensure_files_downloaded(app.state.minio_client, "pd2", CACHE_DIR, REQUIRED_FILES)

    yield

    console.print("[bold red]<<< Shutting down application...[/bold red]")
    app.state.taxi_zones = None
    app.state.minio_client = None

app = FastAPI(lifespan=lifespan)
