select
    cast(id_clientes as number(38,0)) as cliente_id,
    nullif(trim(cliente), '') as nome_cliente,
    nullif(trim(endereco), '') as endereco,
    cast(id_concessionarias as number(38,0)) as concessionaria_id,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'clientes') }}
