"""Gold Layer — Dimensional: modelo estrela para BI."""

import os
import gc
import time
import polars as pl
from rich.console import Console

from src.transform.schemas import (
    PORTE_EMPRESA_MAP,
    IDENTIFICADOR_MATRIZ_FILIAL_MAP,
    SITUACAO_CADASTRAL_MAP,
    IDENTIFICADOR_SOCIO_MAP,
)
from src.transform.gold._io import (
    get_partition_files,
    read_small_table,
    process_partitioned,
    aggregate_partitioned,
)

console = Console()

DOMAIN_TABLES = ["cnaes", "motivos", "naturezas", "paises", "qualificacoes", "municipios"]


def _write_domain_dims(silver_dir: str, output_dir: str):
    """Copia tabelas de domínio como dimensões."""
    for table in DOMAIN_TABLES:
        df = read_small_table(silver_dir, table)
        if df is not None:
            df.write_parquet(os.path.join(output_dir, f"dim_{table}.parquet"))
            console.print(f"  [green]✓[/] dim_{table}.parquet ({len(df):,})")


def _transform_empresa(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social",
        "porte_empresa", "ente_federativo_responsavel",
    ]).with_columns(
        pl.col("porte_empresa")
        .replace_strict(PORTE_EMPRESA_MAP, default="OUTROS")
        .alias("porte_empresa_desc")
    )


def _transform_estabelecimento(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        "cnpj_basico", "cnpj_ordem", "cnpj_dv",
        "identificador_matriz_filial", "nome_fantasia",
        "situacao_cadastral", "data_situacao_cadastral",
        "data_inicio_atividade",
        "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero",
        "complemento", "bairro", "cep", "uf", "municipio",
        "ddd_1", "telefone_1", "correio_eletronico",
    ]).with_columns([
        pl.col("identificador_matriz_filial")
        .replace_strict(IDENTIFICADOR_MATRIZ_FILIAL_MAP, default="OUTROS")
        .alias("tipo_matriz_filial_desc"),
        pl.col("situacao_cadastral")
        .replace_strict(SITUACAO_CADASTRAL_MAP, default="OUTROS")
        .alias("situacao_cadastral_desc"),
        (
            pl.col("cnpj_basico").cast(pl.Utf8)
            + pl.col("cnpj_ordem").cast(pl.Utf8)
            + pl.col("cnpj_dv").cast(pl.Utf8)
        ).alias("cnpj_completo"),
    ])


def _transform_socio(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("identificador_socio")
        .replace_strict(IDENTIFICADOR_SOCIO_MAP, default="OUTROS")
        .alias("tipo_socio_desc")
    )


def _build_fato_empresa(silver_dir: str, output_dir: str):
    """Constrói fato_empresa com agregações partição por partição."""
    estab_agg = aggregate_partitioned(
        silver_dir, "estabelecimentos",
        columns=["cnpj_basico", "situacao_cadastral"],
        agg_exprs=[
            pl.len().alias("qtd_estabelecimentos"),
            pl.col("situacao_cadastral").eq(2).sum().alias("qtd_estabelecimentos_ativos"),
        ],
        final_agg_exprs=[
            pl.col("qtd_estabelecimentos").sum(),
            pl.col("qtd_estabelecimentos_ativos").sum(),
        ],
    )

    socio_agg = aggregate_partitioned(
        silver_dir, "socios",
        columns=["cnpj_basico"],
        agg_exprs=[pl.len().alias("qtd_socios")],
        final_agg_exprs=[pl.col("qtd_socios").sum()],
    )

    simples_files = get_partition_files(silver_dir, "simples")
    simples_df = (
        pl.read_parquet(simples_files, columns=["cnpj_basico", "opcao_pelo_simples", "opcao_mei"])
        if simples_files else None
    )

    empresa_files = get_partition_files(silver_dir, "empresas")
    if not empresa_files:
        console.print("  [yellow]⚠ empresas não encontrada, pulando fato_empresa[/]")
        return

    out_dir = os.path.join(output_dir, "fato_empresa")
    os.makedirs(out_dir, exist_ok=True)
    total_rows = 0
    t0 = time.perf_counter()

    null_fill_cols = ["qtd_estabelecimentos", "qtd_estabelecimentos_ativos", "qtd_socios"]

    for i, pf in enumerate(empresa_files):
        try:
            fato = pl.read_parquet(pf, columns=["cnpj_basico", "capital_social", "porte_empresa"])
            if estab_agg is not None:
                fato = fato.join(estab_agg, on="cnpj_basico", how="left")
            if socio_agg is not None:
                fato = fato.join(socio_agg, on="cnpj_basico", how="left")
            if simples_df is not None:
                fato = fato.join(simples_df, on="cnpj_basico", how="left")

            fato = fato.with_columns([
                pl.col(c).fill_null(0) for c in null_fill_cols if c in fato.columns
            ])
            fato.write_parquet(os.path.join(out_dir, f"part-{i}.parquet"))
            total_rows += len(fato)
            del fato
        except Exception as e:
            console.print(f"    [red]✗ fato_empresa partição {i+1}: {e}[/]")

    elapsed = time.perf_counter() - t0
    console.print(f"  [green]✓[/] fato_empresa/ ({len(empresa_files)} parts, {total_rows:,}) [{elapsed:.1f}s]")
    gc.collect()


def build(silver_dir: str, output_dir: str):
    """Gera modelo estrela completo."""
    console.print("[bold]Dimensional[/]")
    os.makedirs(output_dir, exist_ok=True)

    _write_domain_dims(silver_dir, output_dir)

    naturezas = read_small_table(silver_dir, "naturezas")
    municipios = read_small_table(silver_dir, "municipios")

    process_partitioned(
        silver_dir, "empresas", output_dir, "dim_empresa",
        transform_fn=_transform_empresa,
        join_df=naturezas, join_on="natureza_juridica",
        join_rename={"codigo": "natureza_juridica", "descricao": "natureza_juridica_desc"},
    )
    process_partitioned(
        silver_dir, "estabelecimentos", output_dir, "dim_estabelecimento",
        transform_fn=_transform_estabelecimento,
        join_df=municipios, join_on="municipio",
        join_rename={"codigo": "municipio", "descricao": "municipio_desc"},
    )
    process_partitioned(
        silver_dir, "socios", output_dir, "dim_socio",
        transform_fn=_transform_socio,
    )

    _build_fato_empresa(silver_dir, output_dir)
