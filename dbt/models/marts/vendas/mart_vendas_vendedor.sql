select
    v.vendedor_id,
    ven.nome_vendedor,
    v.concessionaria_id,
    con.nome_concessionaria,
    count(*) as quantidade_vendas,
    sum(v.valor_venda) as valor_total_vendas,
    avg(v.valor_venda) as ticket_medio
from {{ ref('fct_vendas') }} v
left join {{ ref('dim_vendedores') }} ven on v.vendedor_id = ven.vendedor_id
left join {{ ref('dim_concessionarias') }} con on v.concessionaria_id = con.concessionaria_id
group by v.vendedor_id, ven.nome_vendedor, v.concessionaria_id, con.nome_concessionaria
