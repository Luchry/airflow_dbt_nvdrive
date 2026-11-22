from datetime import timedelta
import pendulum

try:
    from airflow.sdk import DAG, get_current_context, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task
    from airflow.operators.python import get_current_context

from carga_postgres_snowflake import TABELAS, carregar_tabela


with DAG(
    dag_id="postgres_to_snowflake",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3, "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=1), "pool": "novadrive_ingestion",
    },
    tags=["novadrive", "ingestao"],
    doc_md="carga diária por timestamp e MERGE",
) as postgres_to_snowflake_etl_dag:
    @task
    def carregar(tabela):
        contexto = get_current_context()
        return carregar_tabela(tabela, contexto["data_interval_end"], contexto["run_id"])

    anterior = None
    for tabela in TABELAS:
        atual = carregar.override(task_id=f"load_{tabela}")(tabela)
        if anterior is not None:
            anterior >> atual
        anterior = atual
