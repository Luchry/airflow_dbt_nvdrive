select
    v.veiculo_id,
    vei.nome_veiculo,
    vei.tipo,
    vei.valor_sugerido as valor_sugerido_atual,
    count(*) as quantidade_vendas,
    sum(v.valor_venda) as valor_total_vendas,
    avg(v.valor_venda) as ticket_medio
from {{ ref('fct_vendas') }} v
left join {{ ref('dim_veiculos') }} vei on v.veiculo_id = vei.veiculo_id
group by v.veiculo_id, vei.nome_veiculo, vei.tipo, vei.valor_sugerido
