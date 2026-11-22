select
    cast(id_estados as number(38,0)) as estado_id,
    nullif(trim(estado), '') as nome_estado,
    upper(nullif(trim(sigla), '')) as sigla,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'estados') }}
