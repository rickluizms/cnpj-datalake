"""
Bronze Layer — Raw ZIP/CSV → Parquet sem transformação.

Lê os CSVs dentro dos ZIPs em data/raw/, aplica apenas os nomes de colunas
do schema, e salva como Parquet em data/bronze/.
"""

import gc
import os
import zipfile

import polars as pl
from rich.console import Console

from src.transform.schemas import (
    FILE_META,
    SCHEMA_COLUMNS,
    CSV_SEPARATOR,
    CSV_ENCODING,
)

console = Console()


def _find_csv_in_zip(zip_ref: zipfile.ZipFile, suffix: str) -> str | None:
    """Encontra o arquivo CSV dentro do ZIP pelo identificador."""
    for name in zip_ref.namelist():
        if suffix.upper() in name.upper():
            return name
    return None


def _process_zip(zip_path: str, table_name: str, columns: list[str], suffix: str, output_dir: str, part_index: int | None = None):
    """Processa um único ZIP: extrai CSV convertendo latin1→utf8 e salva como Parquet."""

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = _find_csv_in_zip(zf, suffix)
            if csv_name is None:
                console.print(f"  [yellow]⚠ CSV com sufixo '{suffix}' não encontrado em {os.path.basename(zip_path)}[/]")
                return False

            # Usa diretório do projeto para temp (evita lotar o drive do sistema)
            os.makedirs(output_dir, exist_ok=True)
            tmp_csv = os.path.join(output_dir, f"_tmp_{os.path.basename(zip_path)}.csv")

            try:
                # Extrai convertendo latin1→utf8 em chunks (evita MemoryError)
                CHUNK = 8 * 1024 * 1024  # 8 MB
                with zf.open(csv_name) as src, open(tmp_csv, "w", encoding="utf-8") as dst:
                    while True:
                        raw = src.read(CHUNK)
                        if not raw:
                            break
                        dst.write(raw.decode("latin-1"))

                # scan_csv (lazy) + streaming collect = memória controlada
                lf = pl.scan_csv(
                    tmp_csv,
                    separator=CSV_SEPARATOR,
                    encoding="utf8",
                    has_header=False,
                    new_columns=columns,
                    infer_schema_length=0,
                    truncate_ragged_lines=True,
                )

                part_suffix = f"part-{part_index}" if part_index is not None else "part-0"
                output_path = os.path.join(output_dir, f"{part_suffix}.parquet")

                # sink_parquet streams direto para disco — nunca materializa o DF inteiro
                lf.sink_parquet(output_path)

                # Libera o handle do CSV antes do cleanup (Windows mantém lock)
                del lf
                gc.collect()

                # Conta linhas do arquivo já salvo (scan é lazy, quase sem memória)
                rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()
                console.print(f"  [green]✓[/] {os.path.basename(zip_path)} → {part_suffix}.parquet ({rows:,} registros)")
                return True

            finally:
                # Libera handles e remove temp (Windows pode manter lock)
                import time as _time
                for _attempt in range(3):
                    try:
                        if os.path.exists(tmp_csv):
                            os.remove(tmp_csv)
                        break
                    except PermissionError:
                        gc.collect()
                        _time.sleep(1)
                else:
                    console.print(f"  [dim yellow]⚠ Não foi possível remover temp: {os.path.basename(tmp_csv)}[/]")

    except Exception as e:
        console.print(f"  [red]✗ Erro processando {os.path.basename(zip_path)}: {type(e).__name__}: {e}[/]")
        import traceback
        console.print(f"  [dim]{traceback.format_exc()}[/]")
        return False


def run(year_month: str):
    """Executa a camada Bronze: Raw → Parquet."""
    raw_dir = os.path.join("data", "raw", year_month)
    bronze_dir = os.path.join("data", "bronze", year_month)

    if not os.path.exists(raw_dir):
        console.print(f"[red]Diretório raw não encontrado: {raw_dir}[/]")
        return

    console.print(f"\n[bold cyan]{'='*60}[/]")
    console.print(f"[bold cyan]  BRONZE LAYER — Raw → Parquet[/]")
    console.print(f"[bold cyan]  Período: {year_month}[/]")
    console.print(f"[bold cyan]{'='*60}[/]\n")

    total_success = 0
    total_errors = 0

    for table_name, meta in FILE_META.items():
        columns = SCHEMA_COLUMNS[table_name]
        suffix = meta["suffix"]
        is_partitioned = meta["partitioned"]
        table_name_lower = table_name.lower()
        output_dir = os.path.join(bronze_dir, table_name_lower)

        console.print(f"[bold]{table_name}[/]")

        if is_partitioned:
            for part in range(10):
                zip_name = f"{table_name}{part}.zip"
                zip_path = os.path.join(raw_dir, zip_name)
                if os.path.exists(zip_path):
                    ok = _process_zip(zip_path, table_name, columns, suffix, output_dir, part_index=part)
                    if ok:
                        total_success += 1
                    else:
                        total_errors += 1
        else:
            zip_name = f"{table_name}.zip"
            zip_path = os.path.join(raw_dir, zip_name)
            if os.path.exists(zip_path):
                ok = _process_zip(zip_path, table_name, columns, suffix, output_dir)
                if ok:
                    total_success += 1
                else:
                    total_errors += 1
            else:
                console.print(f"  [yellow]⚠ Arquivo não encontrado: {zip_name}[/]")

    console.print(f"\n[bold green]Bronze concluído: {total_success} arquivos processados, {total_errors} erros[/]\n")
