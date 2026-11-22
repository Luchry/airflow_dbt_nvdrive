select
    cliente_id,
    nome_cliente,
    endereco,
    concessionaria_id,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__clientes') }}
