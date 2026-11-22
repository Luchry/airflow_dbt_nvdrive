select
    cast(id_cidades as number(38,0)) as cidade_id,
    nullif(trim(cidade), '') as nome_cidade,
    cast(id_estados as number(38,0)) as estado_id,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'cidades') }}
