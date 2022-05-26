with

networth_table as (
    select *
	from  {{ source('dbt','networth') }}
),

most_recent_networth as (
	select *
	from  networth_table
	order by datetime_stamp desc
	limit 1),

networth_with_date_diff as (
    select *
    , DATE_PART('day', (select datetime_stamp from most_recent_networth)::timestamp 
                - datetime_stamp::timestamp) AS date_diff_from_now
    from networth_table),

thirty_days_ago_networth as(
    select *
    from networth_with_date_diff
    where date_diff_from_now >= 30
    order by datetime_stamp desc
    limit 1),

sixty_days_ago_networth as(
    select *
    from networth_with_date_diff
    where date_diff_from_now >= 60
    order by datetime_stamp desc
    limit 1),

networth_comparison AS (
    select 
    (select networth from most_recent_networth) as networth_now,
	(select networth from thirty_days_ago_networth) as networth_thirty_days_ago,
    (select networth from sixty_days_ago_networth) as networth_sixty_days_ago,
    (select investment_amount from most_recent_networth) as investment_amount_now,
	(select investment_amount from thirty_days_ago_networth) as investment_amount_thirty_days_ago,
	(select investment_amount from sixty_days_ago_networth) as investment_amount_sixty_days_ago
)

select *
from networth_comparison
