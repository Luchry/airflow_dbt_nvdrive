select
    cast(id_veiculos as number(38,0)) as veiculo_id,
    nullif(trim(nome), '') as nome_veiculo,
    nullif(trim(tipo), '') as tipo,
    cast(valor as number(10,2)) as valor_sugerido,
    cast(data_inclusao as timestamp_ltz) as data_inclusao,
    cast(coalesce(data_atualizacao, data_inclusao) as timestamp_ltz) as data_atualizacao
from {{ source('postgres', 'veiculos') }}
