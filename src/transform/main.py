"""
Transform — Orquestrador da arquitetura Medallion.

Executa as camadas Bronze → Silver → Gold sequencialmente,
com suporte a execução individual via flags.
"""

import argparse
import sys
import time

from rich.console import Console

console = Console()


def get_year_month() -> str:
    """Retorna o período atual no formato YYYY-MM."""
    return time.strftime("%Y-%m")


def _run_pipeline(year_month: str, run_bronze: bool = True, run_silver: bool = True, run_gold: bool = True):
    """Executa as camadas selecionadas do pipeline."""
    console.print(f"\n[bold magenta]╔{'═'*58}╗[/]")
    console.print(f"[bold magenta]║  CNPJ DATALAKE — Arquitetura Medallion{' '*19}║[/]")
    console.print(f"[bold magenta]║  Período: {year_month}{' '*(47-len(year_month))}║[/]")
    console.print(f"[bold magenta]╚{'═'*58}╝[/]")

    start = time.time()

    if run_bronze:
        from src.transform.bronze import run as bronze_run
        bronze_run(year_month)

    if run_silver:
        from src.transform.silver import run as silver_run
        silver_run(year_month)

    if run_gold:
        from src.transform.gold import run as gold_run
        gold_run(year_month)

    elapsed = time.time() - start
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    console.print(f"\n[bold magenta]Pipeline concluído em {minutes}m {seconds}s[/]\n")


def run_all(year_month: str | None = None):
    """Executa todas as camadas. Chamado pelo main.py raiz."""
    year_month = year_month or get_year_month()
    _run_pipeline(year_month)


def main():
    """Entry point CLI com flags individuais."""
    parser = argparse.ArgumentParser(description="CNPJ Datalake — Transform")
    parser.add_argument("--bronze", action="store_true", help="Executar apenas camada Bronze")
    parser.add_argument("--silver", action="store_true", help="Executar apenas camada Silver")
    parser.add_argument("--gold", action="store_true", help="Executar apenas camada Gold")
    parser.add_argument("--period", type=str, default=None, help="Período YYYY-MM (padrão: mês atual)")
    args = parser.parse_args()

    year_month = args.period or get_year_month()
    any_flag = args.bronze or args.silver or args.gold

    _run_pipeline(
        year_month,
        run_bronze=not any_flag or args.bronze,
        run_silver=not any_flag or args.silver,
        run_gold=not any_flag or args.gold,
    )


if __name__ == "__main__":
    main()
