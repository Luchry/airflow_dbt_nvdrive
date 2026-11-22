select
    v.concessionaria_id,
    con.nome_concessionaria,
    cid.nome_cidade,
    est.nome_estado,
    count(*) as quantidade_vendas,
    sum(v.valor_venda) as valor_total_vendas,
    avg(v.valor_venda) as ticket_medio
from {{ ref('fct_vendas') }} v
left join {{ ref('dim_concessionarias') }} con on v.concessionaria_id = con.concessionaria_id
left join {{ ref('dim_cidades') }} cid on con.cidade_id = cid.cidade_id
left join {{ ref('dim_estados') }} est on cid.estado_id = est.estado_id
group by v.concessionaria_id, con.nome_concessionaria, cid.nome_cidade, est.nome_estado
