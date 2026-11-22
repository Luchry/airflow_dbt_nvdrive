select
    vendedor_id,
    nome_vendedor,
    concessionaria_id,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__vendedores') }}
