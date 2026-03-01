import httpx
import os
import asyncio
import time
from core.progress import create_download_progress

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))  # segundos (base do backoff)

SEMAPHORE = asyncio.Semaphore(MAX_WORKERS)

BASE_HOST = os.getenv("BASE_HOST")
SHARE_ID = os.getenv("SHARE_ID")
WEBDAV_URL = f"{BASE_HOST}/public.php/webdav"

YEAR_MONTH = time.strftime("%Y-%m")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

FILES_CONFIG = [
    # (file_name, is_partitioned: Socios1.zip, Socios2.zip, ... will be downloaded in parallel)
    ("Cnaes.zip", False),
    ("Motivos.zip", False),
    ("Naturezas.zip", False),
    ("Paises.zip", False),
    ("Qualificacoes.zip", False),
    ("Municipios.zip", False),
    ("Simples.zip", False),
    ("Empresas", True),
    ("Socios", True),
    ("Estabelecimentos", True),
]


async def download_via_webdav(client: httpx.AsyncClient, file_name: str, base_dir: str, progress, overall_task_id):
    """Download via WebDAV do Nextcloud (public share) com retry e progress."""
    url = f"{WEBDAV_URL}/{YEAR_MONTH}/{file_name}"
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, file_name)
    task_id = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with SEMAPHORE:
                async with client.stream("GET", url, headers=HEADERS, auth=(SHARE_ID, '')) as response:
                    if response.status_code == 404:
                        progress.advance(overall_task_id)
                        return None

                    response.raise_for_status()

                    total = int(response.headers.get("content-length", 0))

                    if task_id is None:
                        task_id = progress.add_task(file_name, total=total or None)
                    else:
                        # Reset no retry
                        progress.reset(task_id, total=total or None)
                        progress.update(task_id, description=f"[yellow]↻ {file_name} (tentativa {attempt})")

                    with open(file_path, 'wb') as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                progress.advance(task_id, len(chunk))

                    progress.update(task_id, description=f"[green]✓ {file_name}")
                    progress.advance(overall_task_id)
                    return  # Sucesso

        except Exception as e:
            # Remove arquivo parcial
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass

            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * (2 ** (attempt - 1))  # Backoff exponencial
                label = f"[yellow]↻ {file_name} (erro, retry em {delay}s...)"
                if task_id is not None:
                    progress.update(task_id, description=label)
                else:
                    task_id = progress.add_task(label, total=None)
                await asyncio.sleep(delay)
            else:
                # Última tentativa falhou
                label = f"[red]✗ {file_name} ({MAX_RETRIES} tentativas)"
                if task_id is not None:
                    progress.update(task_id, description=label)
                else:
                    progress.add_task(label, total=None)
                progress.advance(overall_task_id)
                raise  # Re-raise para ser capturado pelo gather


async def main():
    base_dir = f"data/raw/{YEAR_MONTH}"

    all_files = []
    for file_name, is_partitioned in FILES_CONFIG:
        if is_partitioned:
            for part in range(10):
                all_files.append(f"{file_name}{part}.zip")
        else:
            all_files.append(file_name)

    start = time.perf_counter()

    with create_download_progress() as progress:
        overall_task_id = progress.add_task(
            f"[cyan]Total ({len(all_files)} arquivos)",
            total=len(all_files),
        )

        async with httpx.AsyncClient(timeout=None) as client:
            tasks = [
                download_via_webdav(client, file_name, base_dir, progress, overall_task_id)
                for file_name in all_files
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            errors = []
            for file_name, result in zip(all_files, results):
                if isinstance(result, Exception):
                    progress.console.print(f"[red]✗ Erro ao processar {file_name}: {result}")
                    errors.append(file_name)

    elapsed = time.perf_counter() - start
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    ok_count = len(all_files) - len(errors)
    from rich.console import Console as _C
    _c = _C()
    _c.print(f"\n[bold magenta]Ingestão concluída em {minutes}m {seconds}s — {ok_count}/{len(all_files)} arquivos baixados[/]\n")


def run():
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())