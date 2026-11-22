-- schema e tabela de controle para watermark no snowflake

CREATE SCHEMA IF NOT EXISTS NOVADRIVE.CONTROL;

CREATE TABLE IF NOT EXISTS NOVADRIVE.CONTROL.INGESTION_WATERMARKS (
    SOURCE_CONNECTION VARCHAR NOT NULL COMMENT 'ID da conexão Postgres no Airflow',
    SOURCE_SCHEMA VARCHAR NOT NULL COMMENT 'Schema da tabela no Postgres',
    SOURCE_TABLE VARCHAR NOT NULL COMMENT 'Nome da tabela de origem',
    TARGET_SCHEMA VARCHAR NOT NULL COMMENT 'Schema raw no Snowflake, em maiusculas',
    WATERMARK TIMESTAMP_TZ NOT NULL COMMENT 'fim exclusivo da última janela diária confirmada',
    LAST_RUN_ID VARCHAR NOT NULL COMMENT 'run_id da DAG diária que confirmou o watermark',
    ROWS_EXTRACTED NUMBER(38, 0) NOT NULL COMMENT 'Linhas extraídas na última janela, incluindo sobreposição',
    LAST_SUCCESS_AT TIMESTAMP_TZ NOT NULL COMMENT 'Instante de confirmação da carga no Snowflake',
    CONSTRAINT INGESTION_WATERMARKS_PK PRIMARY KEY (
        SOURCE_CONNECTION, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA
    )
)
COMMENT = 'Checkpoint por tabela, atualizado na mesma transação do MERGE -- DAG manual não altera este controle';

-- a primeira execução diária deve fazer uma leitura histórica completa
