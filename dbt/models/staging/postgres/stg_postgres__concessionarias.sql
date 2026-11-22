select
    cast(id_concessionarias as number(38,0)) as concessionaria_id,
    nullif(trim(concessionaria), '') as nome_concessionaria,
    cast(id_cidades as number(38,0)) as cidade_id,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'concessionarias') }}
