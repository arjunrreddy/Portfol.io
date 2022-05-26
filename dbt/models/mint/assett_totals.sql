with

-- dbt best practice is to define source models in the `models/sources.yml`
assett_class_split as (
    select * from {{ source('dw_staging',
                            'assett_class_split') }}
),

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
    FROM investment_amounts),

assett_distribution AS (
    SELECT sd.stock_symbol AS stock_symbol
    , total_amount
    , ROUND(us_stock * total_amount,2) AS amount_us_stock
    , ROUND(foreign_stock * total_amount,2) AS amount_foreign_stock
    , ROUND(bonds * total_amount,2) AS amount_bonds
    , ROUND(cryptocurrency * total_amount,2) AS amount_cryptocurrency
    , ROUND(stablecoin * total_amount,2) AS amount_stablecoin
    FROM stock_distribution sd
    LEFT JOIN assett_class_split acs
    ON sd.stock_symbol = acs.stock_symbol),

assett_totals AS (
    SELECT DISTINCT 'us_stock' AS assett_class
        , (SELECT DISTINCT SUM(amount_us_stock) OVER () FROM assett_distribution) AS amount
    FROM assett_distribution
    UNION ALL
    SELECT DISTINCT 'foreign_stock' AS assett_class
        , (SELECT DISTINCT SUM(amount_foreign_stock) OVER () FROM assett_distribution) AS amount
    FROM assett_distribution
    UNION ALL
    SELECT DISTINCT 'bonds' AS assett_class
        , (SELECT DISTINCT SUM(amount_bonds) OVER () FROM assett_distribution) AS amount
    FROM assett_distribution
    UNION ALL
    SELECT DISTINCT 'cryptocurrency' AS assett_class
        , (SELECT DISTINCT SUM(amount_cryptocurrency) OVER () FROM assett_distribution) AS amount
    FROM assett_distribution
    UNION ALL
    SELECT DISTINCT 'stablecoin' AS assett_class
        , (SELECT DISTINCT SUM(amount_stablecoin) OVER () FROM assett_distribution) AS amount
    FROM assett_distribution
)

SELECT *
FROM assett_totals