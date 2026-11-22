select
    venda_id,
    veiculo_id,
    concessionaria_id,
    vendedor_id,
    cliente_id,
    valor_venda,
    data_venda,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__vendas') }}
