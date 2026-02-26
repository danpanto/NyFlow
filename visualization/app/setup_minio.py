import os
import sys
from pathlib import Path
from dotenv import load_dotenv, set_key
from minio import Minio
from minio.error import S3Error
import socket
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.progress import (
    Progress, 
    SpinnerColumn,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TextColumn
)
import hashlib
import asyncio

# Initialize the Rich console
console = Console()

class NetworkReachabilityError(Exception):
    """Raised when the MinIO server cannot be reached at all (e.g., VPN is off)."""
    pass

def test_minio_connection(access_key: str, secret_key: str) -> Minio | None:
    endpoint = "minio.fdi.ucm.es"
    connection_panel = Panel(
        "[bold red]NETWORK REACHABILITY ERROR[/bold red]\n\n"
        f"Target: {endpoint}\n"
        "Status: [yellow]UNREACHABLE[/yellow]\n\n"
        "ADVICE: Please ensure your [bold cyan]VPN is connected[/bold cyan] and active.",
        title="Step 1: Network Check", expand=False
    )    

    # We check if the endpoint is reachable on common ports (443 for HTTPS, 9000 for MinIO API)
    reachable = False
    for port in [443, 9000]:
        try:
            socket.create_connection((endpoint, port), timeout=2)
            reachable = True
            break
        except (socket.timeout, socket.gaierror, ConnectionRefusedError):
            continue

    if not reachable:
        console.print(connection_panel)
        raise NetworkReachabilityError()

    # Authentication
    try:
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True
        )
        # Healthcheck
        client.list_buckets()
        return client

    # Error checking
    except S3Error as e:
        console.print(f"[bold red][!] AUTHENTICATION FAILED:[/bold red] {e.message}")
        return None
    except Exception as e:
        err_str = str(e).lower()
        if "mismatched tag" in err_str or "token" in err_str:
            console.print(connection_panel)
            raise NetworkReachabilityError()
        else:
            console.print(f"[bold red][!] UNEXPECTED ERROR:[/bold red] {e}")
        return None

def save_to_dotenv(access_key, secret_key):
    """Writes or updates the credentials in a .env file."""
    env_path = Path(".env")
    env_path.touch(exist_ok=True)
    
    try:
        set_key(env_path, "MINIO_ACCESS_KEY", access_key)
        set_key(env_path, "MINIO_SECRET_KEY", secret_key)
        console.print("[bold green]>>> Success: Credentials saved to .env file.[/bold green]")
    except Exception as e:
        console.print(f"[bold red][!] Failed to write .env file: {e}[/bold red]")

def load_minio_client() -> Minio:
    """
    Attempts to load MinIO credentials, prompts user if missing,
    auto-detects file vs keys, and allows 3 attempts.
    """

    try:
        # Initial load of default .env
        load_dotenv()

        # Check initial enviromental variables
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")

        if access_key and secret_key:
            # Spinner for network feedback
            status_msg = f"[bold cyan]Attempting connection..."
            with console.status(status_msg, spinner="dots"):
                client = test_minio_connection(access_key, secret_key)

            if client:
                console.print("[bold green]>>> Successfully connected to MinIO![/bold green]\n")
                return client

        # Attempt user provided input
        console.print("[bold yellow][!] minio credentials (minio_access_key/secret_key) not found.[/bold yellow]")

        max_attempts = 3

        for attempt in range(1, max_attempts + 1):

            console.print("\n[bold]Configuration Options:[/bold]")
            console.print(" [cyan]*[/cyan] Provide the path to a valid [green].env[/green] file")
            console.print(" [cyan]*[/cyan] OR type your MinIO [blue]Access Key[/blue] directly")

            user_input = Prompt.ask("[bold magenta]>> Enter file path OR Access Key[/bold magenta]").strip()

            if not user_input:
                continue

            file_path = Path(user_input)
            if file_path.is_file():
                console.print(f"[bold yellow][#] File detected.[/bold yellow] Loading from '[cyan]{file_path}[/cyan]'...")
                load_dotenv(dotenv_path=file_path, override=True)
                # Re-fetch from env immediately
                access_key = os.getenv("MINIO_ACCESS_KEY")
                secret_key = os.getenv("MINIO_SECRET_KEY")
                stdin_input = False
            else:
                console.print("[bold blue][#] Access Key detected.[/bold blue]")
                secret_input = Prompt.ask("[bold magenta]>> Enter MinIO Secret Key[/bold magenta]", password=True).strip()

                access_key = user_input
                secret_key = secret_input
                stdin_input = True
            
            # Try connection
            if access_key and secret_key:
                with console.status("[bold cyan]Validating...", spinner="dots"):
                    client = test_minio_connection(access_key, secret_key)
                
                if client:
                    console.print("[bold green]>>> Connection Verified![/bold green]")
                    if stdin_input and Confirm.ask("[bold yellow]?? Save these to local .env?[/bold yellow]", default=False):
                        save_to_dotenv(access_key, secret_key)
                    return client
            else:
                console.print("[bold red][!] No keys found in provided input.[/bold red]")

        console.print("\n[bold white on red] Error: Initialization failed. Raising Exception. [/bold white on red]")
        raise Exception("Minio Loading Exception")

    except NetworkReachabilityError as e:
        console.print("\n[bold white on red] Error: Network is unreachable. [/bold white on red]")
        raise e

def sync_verify_and_download(client: Minio, bucket: str, obj_name: str, file_path: Path, progress: Progress) -> None:
    task_id = progress.add_task(f"[cyan]Checking {file_path.name}...", total=None)
    
    # 1. Get remote file metadata
    stat = client.stat_object(bucket, obj_name)
    remote_etag = stat.etag.strip('"') 
    
    # Define the sidecar file path
    hash_file_path = file_path.with_suffix(file_path.suffix + ".md5")
    
    # 2. Ultra-Fast Validation (The Sidecar Check)
    if file_path.exists() and hash_file_path.exists():
        with open(hash_file_path, "r") as f:
            local_cached_hash = f.read().strip()
            
        if local_cached_hash == remote_etag:
            progress.update(task_id, description=f"[green]>>> {file_path.name} (Cached)[/green]", completed=stat.size, total=stat.size)
            return

    # 3. Download the file (if missing or hashes don't match)
    progress.update(task_id, description=f"[yellow]Downloading {file_path.name}...", total=stat.size, completed=0)
    
    response = client.get_object(bucket, obj_name)
    try:
        with open(file_path, "wb") as f:
            for chunk in response.stream(32 * 1024):
                f.write(chunk)
                progress.update(task_id, advance=len(chunk))
                
        # 4. Save the sidecar hash file AFTER a successful download
        with open(hash_file_path, "w") as f:
            f.write(remote_etag)
            
    finally:
        response.close()
        response.release_conn()

    progress.update(task_id, description=f"[bold green]>>> {file_path.name} (Complete)[/bold green]")

async def ensure_files_downloaded(client: Minio, bucket: str, cache_dir: Path, required_files: dict[str, str]):
    """Manages the concurrent downloading of all required files."""
    console.print("\n[bold]Synchronizing files with MinIO...[/bold]")
    
    # Setup the beautiful Rich Progress UI
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console
    ) as progress:
        
        tasks = []
        for local_name, remote_path in required_files.items():
            file_path = cache_dir / local_name
            # Send each download to a background thread
            tasks.append(
                asyncio.to_thread(sync_verify_and_download, client, bucket, remote_path, file_path, progress)
            )
        
        # Run them all at the same time
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            console.print(f"\n[bold white on red] FATAL: File synchronization failed: {e} [/bold white on red]")
            sys.tracebacklimit = 0
            raise SystemExit(1)
