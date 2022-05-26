with

-- dbt best practice is to define source models in the `models/sources.yml`
account_data as (
    select * from {{ source('dw_staging',
                            'account_data') }}
),

investment_distribution as (
    select * from {{ source('dw_staging',
                            'investment_distribution') }}
),

most_recent AS (
    select *
    from account_data
    ORDER BY datetime_now DESC
    LIMIT 1),

investment_amounts AS (
    SELECT stock_symbol
    ,{{ var('stock_distribution_invest_amounts_sql') }}
    FROM investment_distribution),

stock_distribution AS (
    SELECT stock_symbol
    , ROUND((m1_diversified_portfolio_spread + m1_roth_ira_spread + m1_tech_portfolio_spread
    + vanguard_brokerage_spread + vanguard_401k_spread + worthy_bonds_spread
    + celsius_ada_spread + celsius_btc_spread + celsius_eth_spread + celsius_usdc_spread
    + coinbase_ada_spread + coinbase_btc_spread + coinbase_eth_spread),2) AS total_amount
    FROM investment_amounts)

SELECT *
FROM stock_distribution