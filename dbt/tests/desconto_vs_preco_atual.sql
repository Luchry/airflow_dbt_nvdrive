{{ config(enabled=var('validate_current_price_discount', false), severity='warn') }}

-- Regra legada, apenas exploratória.. o cadastro não guarda o preço da data da venda
select f.venda_id, f.valor_venda, d.valor_sugerido
from {{ ref('fct_vendas') }} f
join {{ ref('dim_veiculos') }} d on f.veiculo_id = d.veiculo_id
where f.valor_venda < d.valor_sugerido * 0.95
   or f.valor_venda > d.valor_sugerido
