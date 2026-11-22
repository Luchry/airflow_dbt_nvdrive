select
    cidade_id,
    nome_cidade,
    estado_id,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__cidades') }}
