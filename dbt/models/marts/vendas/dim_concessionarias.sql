select
    concessionaria_id,
    nome_concessionaria,
    cidade_id,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__concessionarias') }}
