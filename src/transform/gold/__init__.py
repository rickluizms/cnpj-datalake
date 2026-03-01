"""
Gold Layer — Dados refinados para propósitos específicos.

Gera três conjuntos de dados a partir da Silver:
1. Relacional — tabelas normalizadas para banco de dados SQL
2. Dimensional — estrela (fatos + dimensões) para BI
3. Analytics — wide table desnormalizada para EDA e ML
"""

import os
from rich.console import Console

from src.transform.gold._io import safe_stage
from src.transform.gold import _relacional, _dimensional, _analytics

console = Console()


def run(year_month: str):
    """Executa a camada Gold: Silver → Gold (Relacional + Dimensional + Analytics)."""
    silver_dir = os.path.join("data", "silver", year_month)
    gold_dir = os.path.join("data", "gold", year_month)

    if not os.path.exists(silver_dir):
        console.print(f"[red]Diretório silver não encontrado: {silver_dir}[/]")
        return

    console.print(f"\n[bold cyan]{'='*60}[/]")
    console.print(f"[bold cyan]  GOLD LAYER — Propósitos Específicos[/]")
    console.print(f"[bold cyan]  Período: {year_month}[/]")
    console.print(f"[bold cyan]{'='*60}[/]\n")

    stages = [
        ("Relacional", _relacional.build, silver_dir, os.path.join(gold_dir, "relacional")),
        ("Dimensional", _dimensional.build, silver_dir, os.path.join(gold_dir, "dimensional")),
        ("Analytics", _analytics.build, silver_dir, os.path.join(gold_dir, "analytics")),
    ]

    results = {}
    for stage_name, fn, *args in stages:
        results[stage_name] = safe_stage(stage_name, fn, *args)

    # Resumo final
    console.print(f"\n[bold cyan]{'='*60}[/]")
    console.print(f"[bold cyan]  RESUMO GOLD LAYER[/]")
    console.print(f"[bold cyan]{'='*60}[/]")
    for name, (status, elapsed) in results.items():
        color = "green" if status == "✓" else "red"
        console.print(f"  [{color}]{status} {name} ({elapsed:.1f}s)[/{color}]")

    failed = [n for n, (s, _) in results.items() if s == "✗"]
    if failed:
        console.print(f"\n[bold red]Gold concluído com erros em: {', '.join(failed)}[/]\n")
    else:
        console.print(f"\n[bold green]Gold concluído com sucesso![/]\n")
