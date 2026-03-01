"""
CNPJ Datalake Pipeline DAG — Arquitetura Medallion.

Orquestra o pipeline completo:
  ingest → transform_bronze → transform_silver → transform_gold

Agendamento: dia 20 de cada mês (00:00 UTC).
Também pode ser disparada manualmente via UI.
"""

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from dotenv import load_dotenv

# Garante que variáveis do projeto (.env) estejam disponíveis
load_dotenv()

# Working directory do container — onde volumes são montados
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_year_month(**context) -> str:
    """Retorna year_month via params ou período atual."""
    params = context.get("params", {})
    ym = params.get("year_month", "")
    return ym if ym else time.strftime("%Y-%m")


def _ensure_cwd():
    """Garante que o working directory é AIRFLOW_HOME (onde data/ está montado)."""
    os.chdir(AIRFLOW_HOME)


def _assert_dir_has_files(path: str, label: str):
    """Verifica que um diretório existe e contém arquivos. Levanta exceção se não."""
    if not os.path.exists(path):
        raise AirflowException(f"{label}: diretório não encontrado → {path}")

    # Checa recursivamente se há pelo menos 1 arquivo
    for root, dirs, files in os.walk(path):
        if files:
            return
    raise AirflowException(f"{label}: diretório vazio → {path}")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def task_ingest(**context):
    """Executa a ingestão (download dos ZIPs via WebDAV)."""
    _ensure_cwd()
    year_month = _get_year_month(**context)

    from src.ingest.main import run as ingest_run
    ingest_run(year_month)

    # Validação pós-execução
    raw_dir = os.path.join("data", "raw", year_month)
    _assert_dir_has_files(raw_dir, "Ingest")


def task_transform_bronze(**context):
    """Executa a camada Bronze: Raw ZIP/CSV → Parquet."""
    _ensure_cwd()
    year_month = _get_year_month(**context)

    # Validação pré-execução
    raw_dir = os.path.join("data", "raw", year_month)
    _assert_dir_has_files(raw_dir, "Bronze (pré)")

    from src.transform.bronze import run as bronze_run
    bronze_run(year_month)

    # Validação pós-execução
    bronze_dir = os.path.join("data", "bronze", year_month)
    _assert_dir_has_files(bronze_dir, "Bronze (pós)")


def task_transform_silver(**context):
    """Executa a camada Silver: Limpeza, tipagem e consolidação."""
    _ensure_cwd()
    year_month = _get_year_month(**context)

    bronze_dir = os.path.join("data", "bronze", year_month)
    _assert_dir_has_files(bronze_dir, "Silver (pré)")

    from src.transform.silver import run as silver_run
    silver_run(year_month)

    silver_dir = os.path.join("data", "silver", year_month)
    _assert_dir_has_files(silver_dir, "Silver (pós)")


def task_transform_gold(**context):
    """Executa a camada Gold: Relacional + Dimensional + Analytics."""
    _ensure_cwd()
    year_month = _get_year_month(**context)

    silver_dir = os.path.join("data", "silver", year_month)
    _assert_dir_has_files(silver_dir, "Gold (pré)")

    from src.transform.gold import run as gold_run
    gold_run(year_month)

    gold_dir = os.path.join("data", "gold", year_month)
    _assert_dir_has_files(gold_dir, "Gold (pós)")


# ---------------------------------------------------------------------------
# Stubs para expansão futura
# ---------------------------------------------------------------------------

# def task_load_relacional(**context):
#     """Carrega dados Gold/Relacional no PostgreSQL."""
#     _ensure_cwd()
#     year_month = _get_year_month(**context)
#     from src.load.relacional import run as load_relacional_run
#     load_relacional_run(year_month)


# def task_load_dimensional(**context):
#     """Carrega dados Gold/Dimensional no Data Warehouse."""
#     _ensure_cwd()
#     year_month = _get_year_month(**context)
#     from src.load.dimensional import run as load_dimensional_run
#     load_dimensional_run(year_month)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "cnpj-datalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cnpj_pipeline",
    default_args=default_args,
    description="Pipeline CNPJ Datalake — Ingestão + Medallion (Bronze → Silver → Gold)",
    schedule="0 0 20 * *",  # Dia 20 de cada mês às 00:00 (UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cnpj", "datalake", "medallion"],
    params={"year_month": ""},
    doc_md=__doc__,
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        execution_timeout=timedelta(hours=2),
    )

    bronze = PythonOperator(
        task_id="transform_bronze",
        python_callable=task_transform_bronze,
        execution_timeout=timedelta(hours=1),
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform_silver,
        execution_timeout=timedelta(hours=1),
    )

    gold = PythonOperator(
        task_id="transform_gold",
        python_callable=task_transform_gold,
        execution_timeout=timedelta(hours=1),
    )

    # Pipeline principal
    ingest >> bronze >> silver >> gold

    # Expansão futura:
    # load_rel = PythonOperator(
    #     task_id="load_relacional",
    #     python_callable=task_load_relacional,
    #     execution_timeout=timedelta(hours=1),
    # )
    # load_dim = PythonOperator(
    #     task_id="load_dimensional",
    #     python_callable=task_load_dimensional,
    #     execution_timeout=timedelta(hours=1),
    # )
    # gold >> load_rel
    # gold >> load_dim
