select
    estado_id,
    nome_estado,
    sigla,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__estados') }}
