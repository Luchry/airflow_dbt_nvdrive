select
    cast(date_trunc('month', convert_timezone('{{ var("business_timezone", "America/Sao_Paulo") }}', v.data_venda)) as date) as mes_venda,
    count(*) as quantidade_vendas,
    sum(v.valor_venda) as valor_total_vendas,
    avg(v.valor_venda) as ticket_medio
from {{ ref('fct_vendas') }} v
group by 1
