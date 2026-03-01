"""
Testes de Ingestão — mede o tempo de download de cada arquivo via WebDAV.

Executa a ingestão real (requer variáveis BASE_HOST e SHARE_ID configuradas)
e gera um relatório Rich com duração e tamanho de cada download.

Uso:
    uv run pytest tests/test_ingestion.py -s
"""

import os
import time
import asyncio
import pytest
import httpx
from dotenv import load_dotenv

load_dotenv()
from rich.console import Console
from rich.table import Table

console = Console()

YEAR_MONTH = time.strftime("%Y-%m")
RAW_DIR = os.path.join("data", "raw", YEAR_MONTH)

# Mesma configuração do ingest/main.py
FILES_CONFIG = [
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


def _build_file_list() -> list[str]:
    """Constrói lista completa de arquivos a baixar."""
    files = []
    for name, is_partitioned in FILES_CONFIG:
        if is_partitioned:
            for part in range(10):
                files.append(f"{name}{part}.zip")
        else:
            files.append(name)
    return files


def _format_duration(seconds: float) -> str:
    """Formata duração em formato legível."""
    if seconds >= 60:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:05.2f}s"
    return f"{seconds:.2f}s"


def _format_size(size_bytes: int) -> str:
    """Formata tamanho de arquivo para exibição."""
    if size_bytes >= 1_073_741_824:
        return f"{size_bytes / 1_073_741_824:.2f} GB"
    if size_bytes >= 1_048_576:
        return f"{size_bytes / 1_048_576:.2f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_bytes} B"


# =============================================================================
# FIXTURE: EXECUTA A INGESTÃO COM CRONOMETRAGEM POR ARQUIVO
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def run_ingestion():
    """Executa download de todos os arquivos e cronometra cada um (com retry)."""
    from src.ingest.main import (
        WEBDAV_URL, HEADERS, SHARE_ID, MAX_WORKERS, MAX_RETRIES, RETRY_DELAY,
    )

    semaphore = asyncio.Semaphore(MAX_WORKERS)
    results: list[dict] = []

    async def timed_download(client: httpx.AsyncClient, file_name: str, base_dir: str):
        """Baixa um arquivo com retry e registra tempo + tamanho."""
        url = f"{WEBDAV_URL}/{YEAR_MONTH}/{file_name}"
        os.makedirs(base_dir, exist_ok=True)
        file_path = os.path.join(base_dir, file_name)

        for attempt in range(1, MAX_RETRIES + 1):
            t0 = time.perf_counter()
            try:
                async with semaphore:
                    async with client.stream("GET", url, headers=HEADERS, auth=(SHARE_ID, '')) as response:
                        if response.status_code == 404:
                            results.append({
                                "file": file_name,
                                "status": "não encontrado",
                                "duration": time.perf_counter() - t0,
                                "size": 0,
                                "retries": attempt - 1,
                            })
                            return

                        response.raise_for_status()

                        total = int(response.headers.get("content-length", 0))
                        downloaded = 0

                        with open(file_path, 'wb') as f:
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    downloaded += len(chunk)

                        elapsed = time.perf_counter() - t0
                        actual_size = total if total else downloaded

                        results.append({
                            "file": file_name,
                            "status": "ok",
                            "duration": elapsed,
                            "size": actual_size,
                            "retries": attempt - 1,
                        })

                        retry_info = f" (após {attempt - 1} retry)" if attempt > 1 else ""
                        console.print(
                            f"  [green]✓[/] {file_name} — {_format_size(actual_size)} em {_format_duration(elapsed)}{retry_info}"
                        )
                        return  # Sucesso

            except Exception as e:
                # Remove arquivo parcial
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass

                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    console.print(
                        f"  [yellow]↻[/] {file_name} — tentativa {attempt} falhou, retry em {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    elapsed = time.perf_counter() - t0
                    results.append({
                        "file": file_name,
                        "status": f"erro: {e}",
                        "duration": elapsed,
                        "size": 0,
                        "retries": attempt - 1,
                    })
                    console.print(
                        f"  [red]✗[/] {file_name} — falhou após {MAX_RETRIES} tentativas: {e}"
                    )

    async def run_all():
        all_files = _build_file_list()
        console.print(f"\n[bold cyan]▶ Iniciando download de {len(all_files)} arquivos (max {MAX_RETRIES} tentativas cada)...[/]\n")

        async with httpx.AsyncClient(timeout=None) as client:
            tasks = [
                timed_download(client, file_name, RAW_DIR)
                for file_name in all_files
            ]
            await asyncio.gather(*tasks)

    pipeline_start = time.perf_counter()
    asyncio.run(run_all())
    pipeline_elapsed = time.perf_counter() - pipeline_start

    # Ordena por nome para o relatório
    results.sort(key=lambda r: r["file"])

    yield {
        "files": results,
        "total_elapsed": pipeline_elapsed,
    }


# =============================================================================
# RELATÓRIO RICH AO FINAL DOS TESTES
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def print_ingestion_report(run_ingestion):
    """Gera relatório detalhado ao final dos testes."""
    yield

    data = run_ingestion
    files = data["files"]
    total_elapsed = data["total_elapsed"]

    # --- Tabela detalhada por arquivo ---
    table = Table(
        title=f"⏱  Relatório de Download — Ingestão (Período: {YEAR_MONTH})",
        show_header=True,
    )
    table.add_column("#", justify="right", style="dim")
    table.add_column("Arquivo", justify="left", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Tamanho", justify="right", style="green")
    table.add_column("Duração", justify="right", style="yellow")
    table.add_column("Velocidade", justify="right", style="magenta")
    table.add_column("Retries", justify="right", style="dim")

    total_size = 0
    ok_count = 0
    err_count = 0
    skip_count = 0

    for i, f in enumerate(files, 1):
        status_label = ""
        speed_label = ""

        if f["status"] == "ok":
            status_label = "[green]✓ OK[/]"
            ok_count += 1
            total_size += f["size"]
            if f["duration"] > 0 and f["size"] > 0:
                speed = f["size"] / f["duration"]
                speed_label = f"{_format_size(int(speed))}/s"
        elif f["status"] == "não encontrado":
            status_label = "[yellow]⚠ 404[/]"
            skip_count += 1
        else:
            status_label = f"[red]✗ Erro[/]"
            err_count += 1

        retries = f.get("retries", 0)
        retry_label = f"[yellow]{retries}[/]" if retries > 0 else "0"

        table.add_row(
            str(i),
            f["file"],
            status_label,
            _format_size(f["size"]) if f["size"] > 0 else "—",
            _format_duration(f["duration"]),
            speed_label or "—",
            retry_label,
        )

    table.add_section()
    avg_speed = _format_size(int(total_size / total_elapsed)) + "/s" if total_elapsed > 0 and total_size > 0 else "—"
    total_retries = sum(f.get("retries", 0) for f in files)
    table.add_row(
        "",
        f"[bold]Total ({len(files)} arquivos)[/]",
        f"[bold green]{ok_count}✓[/] [yellow]{skip_count}⚠[/] [red]{err_count}✗[/]",
        f"[bold]{_format_size(total_size)}[/]",
        f"[bold]{_format_duration(total_elapsed)}[/]",
        f"[bold]{avg_speed}[/]",
        f"[bold]{total_retries}[/]",
    )

    console.print("\n")
    console.print(table)
    console.print()


# =============================================================================
# CASOS DE TESTE
# =============================================================================

def test_raw_directory_created():
    """Garante que o diretório de destino foi criado."""
    assert os.path.exists(RAW_DIR), f"Diretório raw {RAW_DIR} não existe após ingestão."


def test_downloaded_files_exist(run_ingestion):
    """Verifica que todos os arquivos com status 'ok' existem no disco."""
    for f in run_ingestion["files"]:
        if f["status"] == "ok":
            path = os.path.join(RAW_DIR, f["file"])
            assert os.path.exists(path), f"Arquivo {f['file']} não encontrado em {RAW_DIR}"


def test_downloaded_files_not_empty(run_ingestion):
    """Garante que nenhum arquivo baixado está vazio (0 bytes)."""
    for f in run_ingestion["files"]:
        if f["status"] == "ok":
            path = os.path.join(RAW_DIR, f["file"])
            size = os.path.getsize(path)
            assert size > 0, f"Arquivo {f['file']} está vazio (0 bytes)."


def test_core_files_downloaded(run_ingestion):
    """Verifica que os arquivos essenciais foram baixados com sucesso."""
    core_prefixes = ["Empresas", "Socios", "Estabelecimentos"]
    ok_files = {f["file"] for f in run_ingestion["files"] if f["status"] == "ok"}

    for prefix in core_prefixes:
        matching = [f for f in ok_files if f.startswith(prefix)]
        assert len(matching) > 0, (
            f"Nenhuma partição de '{prefix}' foi baixada com sucesso."
        )


def test_no_download_errors(run_ingestion):
    """Garante que nenhum download falhou com erro (exceto 404)."""
    errors = [
        f for f in run_ingestion["files"]
        if f["status"] not in ("ok", "não encontrado")
    ]
    assert len(errors) == 0, (
        f"{len(errors)} arquivo(s) com erro: "
        + ", ".join(f'{e["file"]} ({e["status"]})' for e in errors)
    )


def test_ingestion_performance(run_ingestion):
    """Garante que a ingestão completa em tempo razoável (< 60 min)."""
    total = run_ingestion["total_elapsed"]
    max_seconds = 60 * 60  # 60 minutos
    assert total < max_seconds, (
        f"Ingestão demorou {_format_duration(total)}, excedendo o limite de 60 minutos."
    )
