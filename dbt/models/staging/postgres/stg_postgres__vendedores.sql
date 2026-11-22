select
    cast(id_vendedores as number(38,0)) as vendedor_id,
    nullif(trim(nome), '') as nome_vendedor,
    cast(id_concessionarias as number(38,0)) as concessionaria_id,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'vendedores') }}
