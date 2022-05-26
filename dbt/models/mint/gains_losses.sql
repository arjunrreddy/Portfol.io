with

-- dbt best practice is to define source models in the `models/sources.yml`
account_data as (
    select * from {{ source('dw_staging',
                            'account_data') }}
),

dates as (
    select * from {{ source('dw_staging',
                            'dates') }}
),

non_trading_amount_all_values as (
    select primary_key
	, datetime_now
	, date_now
	, {{ var('investment_amount') }} as investment_total
	, ROW_NUMBER() over
        (partition by date_now order by datetime_now desc) as first_pre_trading
	, ROW_NUMBER () over
			(partition by date_now order by datetime_now asc) as first_post_trading
	from account_data
	where datetime_now::time <= '13:30:00.000000'::time
),

pre_trading as (
	select primary_key, date_now, investment_total
	from non_trading_amount_all_values
	where first_pre_trading = 1 and (extract(dow from datetime_now)) in (1,2,3,4,5)
),

post_trading as (
	select primary_key, date_now - INTERVAL '1 DAY' as date_before, investment_total
	from non_trading_amount_all_values
	where first_post_trading = 1 and (extract(dow from datetime_now)) in (2,3,4,5,6)
),

gains_losses_weekday_no_previous as (
    select date_value
    , pre.investment_total as pre_investment
    , post.investment_total as post_investment
    , post.investment_total - pre.investment_total as gains_losses
    from dates
    left join pre_trading pre on dates.date_value = pre.date_now
    left join post_trading post on dates.date_value = post.date_before
	where (extract(dow from date_value)) in (1,2,3,4,5)
),

gains_losses as (
	select *,
	LEAD(gains_losses) OVER(ORDER BY date_value DESC) as previous_day_gains
	from gains_losses_weekday_no_previous
)

SELECT *
FROM gains_losses