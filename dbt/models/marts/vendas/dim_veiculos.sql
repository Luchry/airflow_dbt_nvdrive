select
    veiculo_id,
    nome_veiculo,
    tipo,
    valor_sugerido,
    data_inclusao,
    data_atualizacao
from {{ ref('stg_postgres__veiculos') }}
