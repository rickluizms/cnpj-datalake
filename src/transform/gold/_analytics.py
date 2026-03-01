"""Gold Layer — Analytics: wide table desnormalizada para EDA e ML."""

import os
import gc
import time
import polars as pl
from rich.console import Console

from src.transform.schemas import PORTE_EMPRESA_MAP
from src.transform.gold._io import (
    get_partition_files,
    read_small_table,
    aggregate_partitioned,
)

console = Console()


def _build_matriz_df(silver_dir: str) -> pl.DataFrame | None:
    """Filtra apenas estabelecimentos matriz, partição por partição."""
    files = get_partition_files(silver_dir, "estabelecimentos")
    if not files:
        return None

    try:
        t0 = time.perf_counter()
        parts = []
        for pf in files:
            df = pl.read_parquet(pf)
            part = df.filter(pl.col("identificador_matriz_filial") == 1)
            if len(part) > 0:
                parts.append(part)
            del df, part

        if not parts:
            return None

        matriz = pl.concat(parts)
        del parts
        gc.collect()

        estab_cols = [c for c in matriz.columns if c != "cnpj_basico"]
        matriz = matriz.rename({c: f"estab_{c}" for c in estab_cols})
        console.print(f"    [dim]→ Matriz: {len(matriz):,} [{time.perf_counter() - t0:.1f}s][/]")
        return matriz
    except Exception as e:
        console.print(f"  [red]✗ Erro ao filtrar matriz: {e}[/]")
        return None


def _build_socio_agg(silver_dir: str) -> pl.DataFrame | None:
    """Agrega sócios com informações do sócio principal."""
    return aggregate_partitioned(
        silver_dir, "socios",
        columns=["cnpj_basico", "nome_socio_razao_social", "qualificacao_socio"],
        agg_exprs=[
            pl.len().alias("qtd_socios"),
            pl.col("nome_socio_razao_social").first().alias("socio_principal"),
            pl.col("qualificacao_socio").first().alias("qualificacao_socio_principal"),
        ],
        final_agg_exprs=[
            pl.col("qtd_socios").sum(),
            pl.col("socio_principal").first(),
            pl.col("qualificacao_socio_principal").first(),
        ],
    )


def build(silver_dir: str, output_dir: str):
    """Gera dataset_empresas desnormalizado partição por partição."""
    console.print("[bold]Analytics / ML[/]")
    os.makedirs(output_dir, exist_ok=True)

    empresa_files = get_partition_files(silver_dir, "empresas")
    if not empresa_files:
        console.print("  [red]✗ Tabela empresas obrigatória para Analytics[/]")
        return

    # Tabelas de lookup (pequenas)
    naturezas = read_small_table(silver_dir, "naturezas")
    cnaes = read_small_table(silver_dir, "cnaes")
    municipios = read_small_table(silver_dir, "municipios")
    simples_df = read_small_table(silver_dir, "simples")

    # Tabelas derivadas (grandes, processadas por partição)
    matriz_df = _build_matriz_df(silver_dir)
    socio_agg = _build_socio_agg(silver_dir)

    # Preparar lookups de join
    lookups = {
        "natureza_juridica": (
            naturezas.rename({"codigo": "natureza_juridica", "descricao": "natureza_juridica_desc"})
            if naturezas is not None else None
        ),
        "estab_cnae_fiscal_principal": (
            cnaes.rename({"codigo": "estab_cnae_fiscal_principal", "descricao": "cnae_principal_desc"})
            if cnaes is not None else None
        ),
        "estab_municipio": (
            municipios.rename({"codigo": "estab_municipio", "descricao": "municipio_desc"})
            if municipios is not None else None
        ),
    }

    out_dir = os.path.join(output_dir, "dataset_empresas")
    os.makedirs(out_dir, exist_ok=True)
    total_rows = 0
    t0 = time.perf_counter()

    for i, pf in enumerate(empresa_files):
        try:
            dataset = pl.read_parquet(pf)

            if matriz_df is not None:
                dataset = dataset.join(matriz_df, on="cnpj_basico", how="left")
            if socio_agg is not None:
                dataset = dataset.join(socio_agg, on="cnpj_basico", how="left")
            if simples_df is not None:
                dataset = dataset.join(simples_df, on="cnpj_basico", how="left")

            dataset = dataset.with_columns(
                pl.col("porte_empresa")
                .replace_strict(PORTE_EMPRESA_MAP, default="OUTROS")
                .alias("porte_empresa_desc")
            )

            for join_col, lookup_df in lookups.items():
                if lookup_df is not None and join_col in dataset.columns:
                    dataset = dataset.join(lookup_df, on=join_col, how="left")

            dataset.write_parquet(os.path.join(out_dir, f"part-{i}.parquet"))
            total_rows += len(dataset)
            del dataset
        except Exception as e:
            console.print(f"    [red]✗ dataset_empresas partição {i+1}: {e}[/]")

    elapsed = time.perf_counter() - t0
    console.print(f"  [green]✓[/] dataset_empresas/ ({len(empresa_files)} parts, {total_rows:,}) [{elapsed:.1f}s]")
    gc.collect()
