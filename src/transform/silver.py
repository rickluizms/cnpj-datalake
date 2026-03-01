"""
Silver Layer — Limpeza, tipagem e consolidação.

Lê Parquets da Bronze, aplica tipos corretos, normaliza strings,
trata valores inválidos. Tabelas grandes mantêm partições separadas.
"""

import os
import polars as pl
from rich.console import Console

from src.transform.schemas import (
    FILE_META,
    DATE_COLUMNS,
    FLOAT_COLUMNS,
    INT_COLUMNS,
    TEXT_NORMALIZE_COLUMNS,
)

console = Console()

# Tabelas grandes que devem manter partições separadas (evitar OOM)
LARGE_TABLES = {"empresas", "estabelecimentos", "socios", "simples"}


def _parse_date_column(expr: pl.Expr, col_name: str) -> pl.Expr:
    """Converte coluna de data YYYYMMDD string → Date. Valores inválidos viram null."""
    return (
        pl.when(
            expr.is_null()
            | (expr == "")
            | (expr == "0")
            | (expr == "00000000")
            | (expr.str.len_chars() != 8)
        )
        .then(pl.lit(None))
        .otherwise(expr.str.to_date("%Y%m%d", strict=False))
        .alias(col_name)
    )


def _parse_float_column(expr: pl.Expr, col_name: str) -> pl.Expr:
    """Converte coluna string com vírgula decimal → Float64."""
    return (
        expr
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .alias(col_name)
    )


def _parse_int_column(expr: pl.Expr, col_name: str) -> pl.Expr:
    """Converte coluna string → Int32."""
    return (
        expr
        .cast(pl.Int32, strict=False)
        .alias(col_name)
    )


def _normalize_text(expr: pl.Expr, col_name: str) -> pl.Expr:
    """Trim + uppercase em colunas de texto e remove emojis."""
    return (
        expr
        .str.replace_all(r'\p{Emoji}', '')
        .str.strip_chars()
        .str.to_uppercase()
        .alias(col_name)
    )


def _clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica limpeza e tipagem ao DataFrame baseado nos nomes das colunas."""
    expressions = []

    for col_name in df.columns:
        col_expr = pl.col(col_name)

        if col_name in DATE_COLUMNS:
            expressions.append(_parse_date_column(col_expr, col_name))
        elif col_name in FLOAT_COLUMNS:
            expressions.append(_parse_float_column(col_expr, col_name))
        elif col_name in INT_COLUMNS:
            expressions.append(_parse_int_column(col_expr, col_name))
        elif col_name in TEXT_NORMALIZE_COLUMNS:
            expressions.append(_normalize_text(col_expr, col_name))
        else:
            # Colunas string: remove emojis e aplica strip
            if df.schema[col_name] == pl.Utf8:
                expressions.append(
                    col_expr.str.replace_all(r'\p{Emoji}', '').str.strip_chars().alias(col_name)
                )
            else:
                expressions.append(col_expr)

    return df.select(expressions)


def _process_table(table_name: str, bronze_dir: str, silver_dir: str):
    """Processa uma tabela: lê partições Bronze, limpa e salva na Silver."""
    table_lower = table_name.lower()
    input_dir = os.path.join(bronze_dir, table_lower)

    if not os.path.exists(input_dir):
        console.print(f"  [yellow]Diretório bronze não encontrado: {table_lower}/[/]")
        return

    parquet_files = sorted([
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith(".parquet")
    ])

    if not parquet_files:
        console.print(f"  [yellow]Nenhum Parquet encontrado em {table_lower}/[/]")
        return

    is_large = table_lower in LARGE_TABLES

    if is_large:
        # Tabelas grandes: processar cada partição separadamente
        output_dir = os.path.join(silver_dir, table_lower)
        os.makedirs(output_dir, exist_ok=True)
        total_rows = 0

        for pf in parquet_files:
            part_name = os.path.basename(pf)
            # Lê lazily, aplica limpeza de forma lazy, única de forma lazy,
            # e depois coleta usando o motor de streaming para evitar OOM
            lf = pl.scan_parquet(pf)
            
            # Precisamos limpar passando um schema de fato. 
            # Como _clean_dataframe operava no df materializado (df.columns),
            # adaptaremos iterando sobre colunas do LazyFrame
            columns = lf.collect_schema().names()
            expressions = []
            for col_name in columns:
                col_expr = pl.col(col_name)
                if col_name in DATE_COLUMNS:
                    expressions.append(_parse_date_column(col_expr, col_name))
                elif col_name in FLOAT_COLUMNS:
                    expressions.append(_parse_float_column(col_expr, col_name))
                elif col_name in INT_COLUMNS:
                    expressions.append(_parse_int_column(col_expr, col_name))
                elif col_name in TEXT_NORMALIZE_COLUMNS:
                    expressions.append(_normalize_text(col_expr, col_name))
                else:
                    if lf.collect_schema()[col_name] == pl.Utf8:
                        expressions.append(col_expr.str.replace_all(r'\p{Emoji}', '').str.strip_chars().alias(col_name))
                    else:
                        expressions.append(col_expr)
                        
            lf = lf.select(expressions)
            df = lf.collect(streaming=True)

            output_path = os.path.join(output_dir, part_name)
            df.write_parquet(output_path)
            total_rows += len(df)

        console.print(f"  [green]✓[/] {table_lower}/ ({len(parquet_files)} partições, {total_rows:,} registros total)")
    else:
        # Tabelas pequenas: consolidar em arquivo único
        dfs = [pl.read_parquet(f) for f in parquet_files]
        df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]

        original_count = len(df)
        df = _clean_dataframe(df)
        df = df.unique()
        cleaned_count = len(df)
        removed = original_count - cleaned_count

        os.makedirs(silver_dir, exist_ok=True)
        output_path = os.path.join(silver_dir, f"{table_lower}.parquet")
        df.write_parquet(output_path)

        removed_info = f", {removed:,} duplicatas removidas" if removed > 0 else ""
        console.print(f"  [green]✓[/] {table_lower}.parquet ({cleaned_count:,} registros{removed_info})")


def run(year_month: str):
    """Executa a camada Silver: Bronze → Silver."""
    bronze_dir = os.path.join("data", "bronze", year_month)
    silver_dir = os.path.join("data", "silver", year_month)

    if not os.path.exists(bronze_dir):
        console.print(f"[red]Diretório bronze não encontrado: {bronze_dir}[/]")
        return

    console.print(f"\n[bold cyan]{'='*60}[/]")
    console.print(f"[bold cyan]  SILVER LAYER — Limpeza e Tipagem[/]")
    console.print(f"[bold cyan]  Período: {year_month}[/]")
    console.print(f"[bold cyan]{'='*60}[/]\n")

    import traceback
    
    for table_name in FILE_META:
        console.print(f"[bold]{table_name}[/]")
        try:
            _process_table(table_name, bronze_dir, silver_dir)
        except Exception as e:
            console.print(f"  [bold red]Erro crítico ao processar {table_name}: {e}[/]")
            console.print(f"  [red]{traceback.format_exc()}[/]")

    console.print(f"\n[bold green]Silver concluído![/]\n")
