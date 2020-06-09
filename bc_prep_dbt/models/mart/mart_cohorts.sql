{{
  config(
    materialized = "table",
  )
}}


with base as (
			select
			*
			from {{ ref('stage_base_table') }}
			),
size as (
			select
			date(date_trunc('month', date_acquired)) as cohort_month,
			country,
			count(*) as cohort_size
			from {{ ref('stage_customers') }}
			group by 1,2
			),
orders as (
			SELECT
			date(date_trunc('month', date_acquired)) as cohort_month,
			date(date_trunc('month', date_shipped)) as ship_month,
			country,
			count(*) as amount_orders,
			sum(gross_basket) as revenue,
			count(distinct customer_id) as amount_customer
			from {{ ref('mart_orders') }} 
			group by 1,2,3
			),
subscriptions as (
			select
			date(date_trunc('month', date_acquired)) as cohort_month,
			date(date_trunc('month', start_date)) as subscription_month,
			country,
			count(*) as amount_subscriptions
			from {{ ref('mart_customer_subscriptions')}}
			group by 1,2,3
			),
unsubscriptions as (
			select
			date(date_trunc('month', date_acquired)) as cohort_month,
			date(date_trunc('month', end_date)) as unsubscription_month,
			country,
			count(*) as amount_unsubscriptions
			from {{ ref('mart_customer_subscriptions')}}
			group by 1,2,3
			)
select
b.*,
size.cohort_size,
o.amount_customer,
o.amount_orders,
sum(o.amount_orders) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) as cumulated_amount_orders,
o.revenue,
sum(o.revenue) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) as cumulated_revenue,
s.amount_subscriptions,
sum(s.amount_subscriptions) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) as cumulated_amount_subscriptions,
u.amount_unsubscriptions,
sum(u.amount_unsubscriptions) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) as cumulated_amount_unsubscriptions,
sum(s.amount_subscriptions) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) 
	- sum(u.amount_unsubscriptions) over (partition by b.cohort_month, b.country order by b.lifetime_month asc rows between unbounded preceding and current row) as amount_active_subscriber
from base b 
left join size  
on b.cohort_month = size.cohort_month
and b.country = size.country
left join orders o 
on b.cohort_month = o.cohort_month
and b.action_month = o.ship_month
and b.country = o.country
left join subscriptions s
on b.cohort_month = s.cohort_month
and b.action_month = s.subscription_month
and b.country = s.country
left join unsubscriptions u
on b.cohort_month = u.cohort_month
and b.action_month = u.unsubscription_month
and b.country = u.country