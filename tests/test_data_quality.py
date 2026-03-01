import os
import time
import glob
import pytest
import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()

# Tenta descobrir o período atual na pasta (assumimos que é o único para teste)
raw_base = os.path.join("data", "raw")
try:
    PERIOD = os.listdir(raw_base)[0]
except IndexError:
    PERIOD = "2026-02"

BRONZE_DIR = os.path.join("data", "bronze", PERIOD)
SILVER_DIR = os.path.join("data", "silver", PERIOD)
GOLD_DIR = os.path.join("data", "gold", PERIOD)


# =============================================================================
# FIXTURE: EXECUTA AS TRANSFORMAÇÕES COM CRONOMETRAGEM
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def run_transformations():
    """Executa Bronze → Silver → Gold e cronometra cada etapa."""
    from src.transform.bronze import run as bronze_run
    from src.transform.silver import run as silver_run
    from src.transform.gold import run as gold_run

    timings: dict[str, float] = {}

    # --- Bronze ---
    console.print("\n[bold cyan]▶ Executando camada Bronze...[/]")
    t0 = time.perf_counter()
    bronze_run(PERIOD)
    timings["Bronze"] = time.perf_counter() - t0

    # --- Silver ---
    console.print("\n[bold cyan]▶ Executando camada Silver...[/]")
    t0 = time.perf_counter()
    silver_run(PERIOD)
    timings["Silver"] = time.perf_counter() - t0

    # --- Gold ---
    console.print("\n[bold cyan]▶ Executando camada Gold...[/]")
    t0 = time.perf_counter()
    gold_run(PERIOD)
    timings["Gold"] = time.perf_counter() - t0

    yield timings  # Disponibiliza os tempos para os testes e para o relatório


# =============================================================================
# HELPERS
# =============================================================================

def count_rows_in_dir(dir_path: str) -> int:
    """Conta linhas de todos os parquets num diretório de forma lazy."""
    pattern = os.path.join(dir_path, "*.parquet")
    files = glob.glob(pattern)
    if not files:
        return 0
    return pl.scan_parquet(files).select(pl.len()).collect().item()


def count_rows_in_file(file_path: str) -> int:
    """Conta linhas de um parquet único."""
    if not os.path.exists(file_path):
        return 0
    return pl.scan_parquet(file_path).select(pl.len()).collect().item()


def check_no_nulls(path: str, is_dir: bool, column: str) -> bool:
    """Verifica se há algum valor nulo em uma coluna chave."""
    if is_dir:
        pattern = os.path.join(path, "*.parquet")
        files = glob.glob(pattern)
        if not files:
            return True
        lf = pl.scan_parquet(files)
    else:
        if not os.path.exists(path):
            return True
        lf = pl.scan_parquet(path)

    null_count = lf.filter(pl.col(column).is_null()).select(pl.len()).collect().item()
    return null_count == 0


def _format_duration(seconds: float) -> str:
    """Formata duração em formato legível (ex: 2m 35s ou 0.84s)."""
    if seconds >= 60:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:05.2f}s"
    return f"{seconds:.2f}s"


# =============================================================================
# RELATÓRIO RICH AO FINAL DOS TESTES
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def print_metrics(run_transformations):
    """Gera um relatório de métricas ao final dos testes usando Rich."""
    test_start = time.perf_counter()

    yield  # Aqui rodam os testes

    test_elapsed = time.perf_counter() - test_start
    timings = run_transformations

    # ── Tabela de tempos de transformação ──
    time_table = Table(
        title=f"⏱  Tempo de Processamento por Camada (Período: {PERIOD})",
        show_header=True,
    )
    time_table.add_column("Camada", justify="left", style="cyan")
    time_table.add_column("Duração", justify="right", style="yellow")

    pipeline_total = 0.0
    for layer, elapsed in timings.items():
        pipeline_total += elapsed
        time_table.add_row(layer, _format_duration(elapsed))

    time_table.add_section()
    time_table.add_row("[bold]Total Pipeline[/]", f"[bold]{_format_duration(pipeline_total)}[/]")

    # ── Tabela de contagem de registros ──
    data_table = Table(
        title=f"📊  Registros por Camada (Período: {PERIOD})",
        show_header=True,
    )
    data_table.add_column("Camada", justify="left", style="cyan")
    data_table.add_column("Tabela", justify="left", style="magenta")
    data_table.add_column("Registros", justify="right", style="green")

    # Coletar Bronze
    if os.path.exists(BRONZE_DIR):
        for t in sorted(os.listdir(BRONZE_DIR)):
            rows = count_rows_in_dir(os.path.join(BRONZE_DIR, t))
            data_table.add_row("Bronze", t, f"{rows:,}")

    # Coletar Silver
    if os.path.exists(SILVER_DIR):
        for f in sorted(os.listdir(SILVER_DIR)):
            path = os.path.join(SILVER_DIR, f)
            if os.path.isdir(path):
                rows = count_rows_in_dir(path)
                data_table.add_row("Silver", f, f"{rows:,}")
            elif f.endswith(".parquet"):
                rows = count_rows_in_file(path)
                data_table.add_row("Silver", f.replace(".parquet", ""), f"{rows:,}")

    # Coletar Gold / Analytics
    analytics_path = os.path.join(GOLD_DIR, "analytics", "dataset_empresas")
    if os.path.exists(analytics_path):
        rows = count_rows_in_dir(analytics_path)
        data_table.add_row("Gold (Analytics)", "dataset_empresas (dir)", f"{rows:,}")
    else:
        analytics_file = os.path.join(GOLD_DIR, "analytics", "dataset_empresas.parquet")
        if os.path.exists(analytics_file):
            data_table.add_row("Gold (Analytics)", "dataset_empresas", f"{count_rows_in_file(analytics_file):,}")

    console.print("\n")
    console.print(time_table)
    console.print()
    console.print(data_table)
    console.print(f"\n[bold yellow]Tempo de verificação dos testes: {_format_duration(test_elapsed)}[/]")


# =============================================================================
# CASOS DE TESTE PYTEST
# =============================================================================

def test_directories_exist():
    """Garante que as três camadas de destino foram criadas."""
    assert os.path.exists(BRONZE_DIR), f"Diretório Bronze {BRONZE_DIR} não existe."
    assert os.path.exists(SILVER_DIR), f"Diretório Silver {SILVER_DIR} não existe."
    assert os.path.exists(GOLD_DIR), f"Diretório Gold {GOLD_DIR} não existe."


def test_core_tables_have_records_in_silver():
    """Garante que as tabelas essenciais receberam dados e tem volume razoável."""
    base_tables = ["empresas", "socios", "estabelecimentos"]

    for tbl in base_tables:
        t_dir = os.path.join(SILVER_DIR, tbl)
        if os.path.isdir(t_dir):
            rows = count_rows_in_dir(t_dir)
        else:
            t_file = os.path.join(SILVER_DIR, f"{tbl}.parquet")
            rows = count_rows_in_file(t_file)

        assert rows > 0, f"A tabela Silver '{tbl}' não possui registros ou não foi encontrada."


def test_no_null_primary_keys_silver():
    """Verifica inconsistências graves de PK (CNPJ deve estar presente nas tabelas principais)."""

    # Empresas
    e_dir = os.path.join(SILVER_DIR, "empresas")
    if os.path.exists(e_dir):
        assert check_no_nulls(e_dir, True, "cnpj_basico"), "Empresas com cnpj_basico nulo."

    # Estabelecimentos
    est_dir = os.path.join(SILVER_DIR, "estabelecimentos")
    if os.path.exists(est_dir):
        assert check_no_nulls(est_dir, True, "cnpj_basico"), "Estabelecimentos com cnpj_basico nulo."

    # Dataset Analytics (Gold)
    a_dir = os.path.join(GOLD_DIR, "analytics", "dataset_empresas")
    if os.path.exists(a_dir):
        assert check_no_nulls(a_dir, True, "cnpj_basico"), "Dataset Analítico com cnpj_basico nulo."


def test_gold_dimensional_schema():
    """Verifica se o dataset gold dimensões contém as tabelas estrela básicas."""
    dim_dir = os.path.join(GOLD_DIR, "dimensional")
    assert os.path.exists(dim_dir), "Camada Gold/dimensional inexistente."

    # Verifica a existência dos arquivos / diretórios
    fatos_e_dims = [
        "fato_empresa",
        "dim_empresa",
        "dim_estabelecimento",
        "dim_municipio.parquet"
    ]

    for obj in fatos_e_dims:
        path = os.path.join(dim_dir, obj)
        assert os.path.exists(path), f"Objeto dimensional esperado não encontrado: {obj}"


def test_transformation_performance(run_transformations):
    """Garante que o pipeline completo executou em tempo razoável (< 30 min)."""
    total = sum(run_transformations.values())
    max_seconds = 30 * 60  # 30 minutos
    assert total < max_seconds, (
        f"Pipeline demorou {_format_duration(total)}, excedendo o limite de 30 minutos."
    )
