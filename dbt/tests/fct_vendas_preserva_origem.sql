-- Cada ID e valor da staging deve permanecer na fato, sem joins filtrando vendas
with origem as (
    select venda_id, valor_venda from {{ ref('stg_postgres__vendas') }}
), fato as (
    select venda_id, valor_venda from {{ ref('fct_vendas') }}
)
select coalesce(o.venda_id, f.venda_id) as venda_id
from origem o
full outer join fato f on o.venda_id = f.venda_id
where o.venda_id is null or f.venda_id is null
   or o.valor_venda is distinct from f.valor_venda
