"""Utilitários de I/O compartilhados para a Gold Layer."""

import os
import gc
import glob
import time
import traceback
import polars as pl
from rich.console import Console

console = Console()


def get_partition_files(silver_dir: str, table: str) -> list[str]:
    """Retorna lista de arquivos parquet de uma tabela Silver."""
    dir_path = os.path.join(silver_dir, table)
    if os.path.isdir(dir_path):
        return sorted(glob.glob(os.path.join(dir_path, "*.parquet")))
    single = os.path.join(silver_dir, f"{table}.parquet")
    return [single] if os.path.isfile(single) else []


def read_small_table(silver_dir: str, table: str) -> pl.DataFrame | None:
    """Lê tabela pequena de domínio para memória."""
    files = get_partition_files(silver_dir, table)
    if not files:
        return None
    try:
        return pl.read_parquet(files)
    except Exception as e:
        console.print(f"  [red]✗ Erro ao ler {table}: {e}[/]")
        return None


def process_partitioned(
    silver_dir: str,
    table: str,
    output_dir: str,
    output_name: str,
    transform_fn=None,
    join_df: pl.DataFrame | None = None,
    join_on: str | None = None,
    join_rename: dict | None = None,
    read_columns: list[str] | None = None,
):
    """
    Processa tabela partição por partição:
    lê → transforma → join opcional → salva.
    """
    files = get_partition_files(silver_dir, table)
    if not files:
        console.print(f"  [yellow]⚠ {table} não encontrado[/]")
        return

    out_dir = os.path.join(output_dir, output_name)
    os.makedirs(out_dir, exist_ok=True)

    join_ready = None
    if join_df is not None and join_rename is not None:
        join_ready = join_df.rename(join_rename)

    total_rows = 0
    t0 = time.perf_counter()

    for i, pf in enumerate(files):
        try:
            df = pl.read_parquet(pf, columns=read_columns)
            if transform_fn is not None:
                df = transform_fn(df)
            if join_ready is not None and join_on is not None:
                df = df.join(join_ready, on=join_on, how="left")
            df.write_parquet(os.path.join(out_dir, f"part-{i}.parquet"))
            total_rows += len(df)
            del df
        except Exception as e:
            console.print(f"    [red]✗ {output_name} partição {i+1}: {e}[/]")

    elapsed = time.perf_counter() - t0
    console.print(f"  [green]✓[/] {output_name}/ ({len(files)} parts, {total_rows:,}) [{elapsed:.1f}s]")
    gc.collect()


def aggregate_partitioned(
    silver_dir: str,
    table: str,
    columns: list[str],
    agg_exprs: list,
    final_agg_exprs: list | None = None,
) -> pl.DataFrame | None:
    """
    Agrega tabela partição por partição e combina resultados.
    
    1. Lê cada partição com apenas `columns`
    2. Aplica group_by("cnpj_basico") + agg_exprs
    3. Concatena tudo e re-agrega com final_agg_exprs (ou agg_exprs)
    """
    files = get_partition_files(silver_dir, table)
    if not files:
        return None

    try:
        t0 = time.perf_counter()
        parts = []
        for pf in files:
            df = pl.read_parquet(pf, columns=columns)
            parts.append(df.group_by("cnpj_basico").agg(agg_exprs))
            del df

        result = pl.concat(parts).group_by("cnpj_basico").agg(
            final_agg_exprs or agg_exprs
        )
        del parts
        gc.collect()
        elapsed = time.perf_counter() - t0
        console.print(f"    [dim]→ Agregação {table}: {len(result):,} [{elapsed:.1f}s][/]")
        return result
    except Exception as e:
        console.print(f"  [red]✗ Erro ao agregar {table}: {e}[/]")
        console.print(f"  [red]{traceback.format_exc()}[/]")
        return None


def safe_stage(name: str, fn, *args, **kwargs):
    """Executa fn com tratamento de erro e cronometragem."""
    t0 = time.perf_counter()
    try:
        fn(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        console.print(f"\n[green]  ✓ {name} concluído em {elapsed:.1f}s[/]\n")
        return ("✓", elapsed)
    except Exception as e:
        elapsed = time.perf_counter() - t0
        console.print(f"\n[red]  ✗ {name} FALHOU após {elapsed:.1f}s: {e}[/]")
        console.print(f"[red]{traceback.format_exc()}[/]\n")
        return ("✗", elapsed)
