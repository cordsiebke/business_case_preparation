with base as (
			select
			*
			from {{ ref('stage_base_table') }}
			),
orders as (
			SELECT
			date(date_trunc('month', date_acquired)) as cohort_month,
			date(date_trunc('month', date_shipped)) as ship_month,
			country,
			count(*) as amount_boxes,
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
o.amount_boxes,
o.revenue,
o.amount_customer,
s.amount_subscriptions,
u.amount_unsubscriptions
from base b 
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