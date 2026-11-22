select
    cast(id_vendas as number(38,0)) as venda_id,
    cast(id_veiculos as number(38,0)) as veiculo_id,
    cast(id_concessionarias as number(38,0)) as concessionaria_id,
    cast(id_vendedores as number(38,0)) as vendedor_id,
    cast(id_clientes as number(38,0)) as cliente_id,
    cast(valor_pago as number(10,2)) as valor_venda,
    cast(data_venda as timestamp_ltz) as data_venda,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'vendas') }}
