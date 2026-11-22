from datetime import timedelta
import pendulum

try:
    from airflow.sdk import DAG, Param, get_current_context, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

from carga_postgres_snowflake import TABELAS, carregar_tabela


with DAG(
    dag_id="novadrive_reprocessamento",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3, "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=3), "pool": "novadrive_ingestion",
    },
    params={
        "tables": Param(["vendas"], type="array", minItems=1, uniqueItems=True,
                        items={"type": "string", "enum": list(TABELAS)}),
        "mode": Param("full", type="string", enum=["full", "range"]),
        "start": Param(None, type=["null", "string"]),
        "end": Param(None, type=["null", "string"]),
    },
    tags=["novadrive", "reprocessamento"],
    doc_md="Reprocessa o estado atual da origem por MERGE, sem alterar watermarks nem replicar exclusões, o retry repete a seleção inteira",
) as novadrive_reprocessamento_dag:
    @task
    def reprocessar():
        contexto = get_current_context()
        parametros = contexto["params"]
        selecionadas = parametros["tables"]
        if not selecionadas or len(selecionadas) != len(set(selecionadas)) or set(selecionadas) - TABELAS.keys():
            raise ValueError("tabelas inválidas ou com duplicações")
        if parametros["mode"] == "range" and (parametros["start"] is None or parametros["end"] is None):
            raise ValueError("O modo range exige start e end com fuso explícito.")
        fim = parametros["end"] or contexto["dag_run"].start_date
        for tabela in TABELAS:
            if tabela in selecionadas:
                carregar_tabela(tabela, fim, contexto["run_id"],
                                modo=parametros["mode"], inicio=parametros["start"])

    reprocessar()
