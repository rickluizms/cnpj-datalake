"""
Report de Métricas — Exibe o tamanho e volume das camadas do Datalake
"""

import os
import time
import glob
import sys
import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

def get_year_month() -> str:
    raw_base = os.path.join("data", "raw")
    if os.path.exists(raw_base):
        folders = os.listdir(raw_base)
        if folders:
            return folders[0]
    return time.strftime("%Y-%m")

def count_rows_lazy(path: str, is_dir: bool) -> int:
    try:
        if is_dir:
            pattern = os.path.join(path, "*.parquet")
            files = glob.glob(pattern)
            if not files:
                return 0
            return pl.scan_parquet(files).select(pl.len()).collect().item()
        else:
            if not os.path.exists(path):
                return 0
            return pl.scan_parquet(path).select(pl.len()).collect().item()
    except Exception as e:
        console.print(f"[red]Erro lendo {path}: {e}[/]")
        return 0

def run():
    start_time = time.time()
    
    period = get_year_month()
    console.print(Panel(f"[bold cyan]Métricas do Datalake — Período {period}[/]", expand=False))
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Camada")
    table.add_column("Tabela")
    table.add_column("Registros", justify="right")
    table.add_column("Formato", justify="right")
    
    base_dir = "data"
    
    # BRONZE
    bronze_dir = os.path.join(base_dir, "bronze", period)
    if os.path.exists(bronze_dir):
        for t in sorted(os.listdir(bronze_dir)):
            t_path = os.path.join(bronze_dir, t)
            count = count_rows_lazy(t_path, True)
            table.add_row("[yellow]Bronze[/]", t, f"{count:,}", "Parquet (Pasta)")
            
    # SILVER
    silver_dir = os.path.join(base_dir, "silver", period)
    if os.path.exists(silver_dir):
        table.add_section()
        for t in sorted(os.listdir(silver_dir)):
            t_path = os.path.join(silver_dir, t)
            is_dir = os.path.isdir(t_path)
            count = count_rows_lazy(t_path, is_dir)
            name = t if is_dir else t.replace(".parquet", "")
            fmt = "Parquet (Pasta)" if is_dir else "Parquet (Arquivo)"
            table.add_row("[silver]Silver[/]", name, f"{count:,}", fmt)
            
    # GOLD
    gold_dir = os.path.join(base_dir, "gold", period)
    if os.path.exists(gold_dir):
        table.add_section()
        for sub in ["relacional", "dimensional", "analytics"]:
            sub_dir = os.path.join(gold_dir, sub)
            if os.path.exists(sub_dir):
                for t in sorted(os.listdir(sub_dir)):
                    t_path = os.path.join(sub_dir, t)
                    is_dir = os.path.isdir(t_path)
                    count = count_rows_lazy(t_path, is_dir)
                    name = t if is_dir else t.replace(".parquet", "")
                    fmt = "Parquet (Pasta)" if is_dir else "Parquet (Arquivo)"
                    table.add_row(f"[gold3]Gold ({sub})[/]", name, f"{count:,}", fmt)
                    
    console.print(table)
    elapsed = time.time() - start_time
    console.print(f"\n[green]Tempo para calcular volumetria: {elapsed:.2f}s[/]\n")

if __name__ == "__main__":
    run()
