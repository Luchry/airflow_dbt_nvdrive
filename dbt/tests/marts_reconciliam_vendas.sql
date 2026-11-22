-- Detecta perdas ou multiplicações de vendas nos quatro agrupamentos
with esperado as (
    select count(*) as quantidade, coalesce(sum(valor_venda), 0) as valor
    from {{ ref('fct_vendas') }}
), observado as (
    {% for model in ['mart_vendas_concessionaria', 'mart_vendas_vendedor', 'mart_vendas_veiculo', 'mart_vendas_mensal'] %}
    select '{{ model }}' as modelo,
           coalesce(sum(quantidade_vendas), 0) as quantidade,
           coalesce(sum(valor_total_vendas), 0) as valor
    from {{ ref(model) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select o.*
from observado o cross join esperado e
where o.quantidade <> e.quantidade or o.valor <> e.valor
