with

-- dbt best practice is to define source models in the `models/sources.yml`
networth_historical as (

    select * from {{ source('dw_staging',
                            'networth_historical') }}

),

account_data as (
    select * from {{ source('dw_staging',
                            'account_data') }}
),

networth_cte as (
    select datetime_now as datetime_stamp 
        ,date_now as date_stamp 
        ,networth as networth
        ,{{ var('pre_investment_amount') }}  as investment_amount
    from networth_historical
    UNION ALL
    select
        datetime_now as datetime_stamp
        ,date_now as date_stamp
        ,{{ var('networth_sql') }} as networth
        ,{{ var('investment_amount') }} as investment_amount
    from account_data
)

select * from networth_cte
