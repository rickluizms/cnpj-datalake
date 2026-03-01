"""Gold Layer — Relacional: tabelas normalizadas para banco SQL."""

import os
import glob
import shutil
import polars as pl
from rich.console import Console

from src.transform.gold._io import get_partition_files

console = Console()

ALL_TABLES = [
    "empresas", "estabelecimentos", "socios", "simples",
    "cnaes", "motivos", "naturezas", "paises", "qualificacoes", "municipios",
]


def build(silver_dir: str, output_dir: str):
    """Copia tabelas Silver → Gold/Relacional sem transformação."""
    console.print("[bold]Relacional[/]")
    os.makedirs(output_dir, exist_ok=True)

    for table in ALL_TABLES:
        try:
            dir_path = os.path.join(silver_dir, table)
            single_path = os.path.join(silver_dir, f"{table}.parquet")

            if os.path.isdir(dir_path):
                out_table_dir = os.path.join(output_dir, table)
                os.makedirs(out_table_dir, exist_ok=True)
                files = glob.glob(os.path.join(dir_path, "*.parquet"))
                for pf in files:
                    shutil.copy2(pf, os.path.join(out_table_dir, os.path.basename(pf)))
                total = sum(
                    pl.scan_parquet(f).select(pl.len()).collect().item() for f in files
                ) if files else 0
                console.print(f"  [green]✓[/] {table}/ ({len(files)} parts, {total:,})")

            elif os.path.isfile(single_path):
                shutil.copy2(single_path, os.path.join(output_dir, f"{table}.parquet"))
                total = pl.scan_parquet(single_path).select(pl.len()).collect().item()
                console.print(f"  [green]✓[/] {table}.parquet ({total:,})")
            else:
                console.print(f"  [yellow]⚠ {table} não encontrado[/]")
        except Exception as e:
            console.print(f"  [red]✗ relacional/{table}: {e}[/]")
