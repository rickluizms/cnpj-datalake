import httpx
import os
import asyncio
import time
from core.progress import create_download_progress

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
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
    """Download via WebDAV do Nextcloud (public share) com progress."""
    url = f"{WEBDAV_URL}/{YEAR_MONTH}/{file_name}"

    async with SEMAPHORE:
        async with client.stream("GET", url, headers=HEADERS, auth=(SHARE_ID, '')) as response:
            if response.status_code == 404:
                progress.advance(overall_task_id)
                return None

            response.raise_for_status()

            total = int(response.headers.get("content-length", 0))
            task_id = progress.add_task(file_name, total=total or None)

            os.makedirs(base_dir, exist_ok=True)
            file_path = os.path.join(base_dir, file_name)

            with open(file_path, 'wb') as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.advance(task_id, len(chunk))

            progress.update(task_id, description=f"[green]✓ {file_name}")
            progress.advance(overall_task_id)


async def main():
    base_dir = f"data/raw/{YEAR_MONTH}"

    all_files = []
    for file_name, is_partitioned in FILES_CONFIG:
        if is_partitioned:
            for part in range(10):
                all_files.append(f"{file_name}{part}.zip")
        else:
            all_files.append(file_name)

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

            for file_name, result in zip(all_files, results):
                if isinstance(result, Exception):
                    progress.console.print(f"[red]✗ Erro ao processar {file_name}: {result}")
                    progress.advance(overall_task_id)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())