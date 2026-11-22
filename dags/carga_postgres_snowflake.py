from contextlib import closing
from datetime import datetime, timedelta, timezone
import logging
import os

TABELAS = {
    "veiculos": ["id_veiculos", "nome", "tipo", "valor", "data_inclusao", "data_atualizacao"],
    "estados": ["id_estados", "estado", "sigla", "data_inclusao", "data_atualizacao"],
    "cidades": ["id_cidades", "cidade", "id_estados", "data_inclusao", "data_atualizacao"],
    "concessionarias": ["id_concessionarias", "concessionaria", "id_cidades", "data_inclusao", "data_atualizacao"],
    "vendedores": ["id_vendedores", "nome", "id_concessionarias", "data_inclusao", "data_atualizacao"],
    "clientes": ["id_clientes", "cliente", "endereco", "id_concessionarias", "data_inclusao", "data_atualizacao"],
    "vendas": ["id_vendas", "id_veiculos", "id_concessionarias", "id_vendedores", "id_clientes",
               "valor_pago", "data_venda", "data_inclusao", "data_atualizacao"],
}

def carregar_tabela(tabela, fim, run_id, modo="incremental", inicio=None):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    datas = []
    for valor in (inicio, fim):
        if isinstance(valor, str):
            valor = datetime.fromisoformat(valor.replace("Z", "+00:00"))
        if valor is not None:
            if not isinstance(valor, datetime) or valor.tzinfo is None or valor.utcoffset() is None:
                raise ValueError("Informe datas com fuso, por exemplo 2026-11-01T00:00:00-03:00")
            valor = valor.astimezone(timezone.utc)
        datas.append(valor)
    inicio, fim = datas
    if fim is None or (modo == "range" and (inicio is None or inicio >= fim)):
        raise ValueError("Informe fim e, no modo range, inicio anterior ao fim")
    if modo != "range" and inicio is not None:
        raise ValueError("inicio só pode ser informado no modo range")

    pg_id = os.getenv("NOVADRIVE_POSTGRES_CONN_ID", "postgres")
    sf_id = os.getenv("NOVADRIVE_SNOWFLAKE_CONN_ID", "snowflake")
    schema_pg = os.getenv("NOVADRIVE_POSTGRES_SCHEMA", "public")
    banco = os.getenv("NOVADRIVE_SNOWFLAKE_DATABASE", "NOVADRIVE").upper()
    schema_raw = os.getenv("NOVADRIVE_RAW_SCHEMA", "STAGE").upper()
    schema_controle = os.getenv("NOVADRIVE_CONTROL_SCHEMA", "CONTROL").upper()
    fuso_origem = os.getenv("NOVADRIVE_SOURCE_TIMEZONE")
    tamanho_lote = int(os.getenv("NOVADRIVE_BATCH_SIZE", "1000"))
    sobreposicao_horas = int(os.getenv("NOVADRIVE_LOOKBACK_HOURS", "24"))

    origem = f'"{schema_pg}"."{tabela}"'
    destino = f'"{banco}"."{schema_raw}"."{tabela.upper()}"'
    temporaria = f'"{banco}"."{schema_raw}"."TMP_CARGA_{tabela.upper()}"'
    controle = f'"{banco}"."{schema_controle}"."INGESTION_WATERMARKS"'
    chave = f'"ID_{tabela.upper()}"'
    filtro_controle = "SOURCE_CONNECTION = %s AND SOURCE_SCHEMA = %s AND SOURCE_TABLE = %s AND TARGET_SCHEMA = %s"
    valores_controle = (pg_id, schema_pg, tabela, schema_raw)

    with closing(PostgresHook(postgres_conn_id=pg_id).get_conn()) as pg, \
            closing(SnowflakeHook(snowflake_conn_id=sf_id).get_conn()) as sf:
        pg.autocommit = False
        sf.autocommit(True)
        with closing(pg.cursor()) as pg_cursor, closing(sf.cursor()) as sf_cursor:
            try:
                if modo == "incremental":
                    sf_cursor.execute(f"SELECT WATERMARK FROM {controle} WHERE {filtro_controle}", valores_controle)
                    registros = sf_cursor.fetchmany(2)
                    if len(registros) > 1:
                        raise ValueError("Watermark duplicado na tabela de controle.")
                    watermark = registros[0][0] if registros else None
                    if watermark is not None:
                        if fim <= watermark:
                            return {"table": tabela, "status": "already_confirmed", "rows_extracted": 0}
                        inicio = watermark - timedelta(hours=sobreposicao_horas)

                pg_cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                pg_cursor.execute("SELECT set_config('TimeZone', %s, true)", (fuso_origem or "UTC",))
                pg_cursor.execute("SELECT CURRENT_TIMESTAMP")
                if fim > pg_cursor.fetchone()[0]:
                    raise ValueError("O fim da janela está no futuro em relação ao postgres")
                pg_cursor.execute(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                    (schema_pg, tabela),
                )

                pg_cursor.execute(
                    f'SELECT 1 FROM {origem} WHERE "id_{tabela}" IS NULL OR coalesce(data_atualizacao, data_inclusao) IS NULL LIMIT 1')
                if pg_cursor.fetchone():
                    raise ValueError(f"{tabela}: registro sem ID ou sem timestamp de inclusão/atualização")
                
                pg_cursor.execute(f'SELECT 1 FROM {origem} GROUP BY "id_{tabela}" HAVING count(*) > 1 LIMIT 1')
                if pg_cursor.fetchone():
                    raise ValueError(f"{tabela}: IDs duplicados no Postgres.")

                # carga temporária fora da transação que altera o destino
                sf_cursor.execute(f"CREATE TEMPORARY TABLE {temporaria} LIKE {destino}")
                colunas_pg = ", ".join(
                    f'"{coluna}"::timestamptz AS "{coluna}"' if coluna.startswith("data_") else f'"{coluna}"'
                    for coluna in TABELAS[tabela]
                )

                colunas_sf = ", ".join(f'"{coluna.upper()}"' for coluna in TABELAS[tabela])
                parametros = ", ".join(["%s"] * len(TABELAS[tabela]))
                timestamp_alteracao = "coalesce(data_atualizacao::timestamptz, data_inclusao::timestamptz)"
                consulta = f"SELECT {colunas_pg} FROM {origem} WHERE {timestamp_alteracao} < %s"
                limites = [fim]
                if inicio is not None:
                    consulta += f" AND {timestamp_alteracao} >= %s"
                    limites.append(inicio)
                quantidade = 0
                with closing(pg.cursor(name="novadrive_extracao")) as extracao:
                    extracao.execute(consulta, tuple(limites))
                    while True:
                        lote = extracao.fetchmany(tamanho_lote)
                        if not lote:
                            break
                        sf_cursor.executemany(f"INSERT INTO {temporaria} ({colunas_sf}) VALUES ({parametros})", lote)
                        quantidade += len(lote)

                atualizacoes = ", ".join(
                    f't."{coluna.upper()}" = s."{coluna.upper()}"'
                    for coluna in TABELAS[tabela] if coluna != f"id_{tabela}"
                )
                valores = ", ".join(f's."{coluna.upper()}"' for coluna in TABELAS[tabela])
                sf_cursor.execute("BEGIN TRANSACTION")
                sf_cursor.execute(f"""
                    MERGE INTO {destino} t USING {temporaria} s ON t.{chave} = s.{chave}
                    WHEN MATCHED AND (
                        coalesce(t.DATA_ATUALIZACAO, t.DATA_INCLUSAO) IS NULL OR
                        coalesce(s.DATA_ATUALIZACAO, s.DATA_INCLUSAO) >= coalesce(t.DATA_ATUALIZACAO, t.DATA_INCLUSAO)
                    ) THEN UPDATE SET {atualizacoes}
                    WHEN NOT MATCHED THEN INSERT ({colunas_sf}) VALUES ({valores})
                """)
                if modo == "incremental":
                    # dados e checkpoint são confirmados ou desfeitos juntos
                    sf_cursor.execute(f"DELETE FROM {controle} WHERE {filtro_controle}", valores_controle)
                    sf_cursor.execute(f"""
                        INSERT INTO {controle} (
                            SOURCE_CONNECTION, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA,
                            WATERMARK, LAST_RUN_ID, ROWS_EXTRACTED, LAST_SUCCESS_AT
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """, (*valores_controle, fim, run_id, quantidade))
                sf.commit()
            except Exception:
                sf.rollback()
                raise

    logging.info("Tabela=%s modo=%s linhas=%s inicio=%s fim=%s", tabela, modo, quantidade, inicio, fim)
    return {"table": tabela, "status": "committed", "rows_extracted": quantidade}
