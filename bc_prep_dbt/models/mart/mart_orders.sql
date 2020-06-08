with orders as (
		select
		*
		from {{ ref('stage_orders') }}
		)
,
customers as (
		select
		*
		from {{ ref('stage_customers') }}
		)
,
customer_subscriptions as (
		select
		*
		from {{ ref('stage_customer_subscriptions') }}
		)
select
c.*,
cs.start_date,
cs.end_date,
o.order_migration_flag,
o.date_shipped,
o.total_amount_billed_retail_gross as gross_basket
from orders o
join customers c 
on o.customer_id = c.customer_id
left join customer_subscriptions cs
on c.id = cs.customer_id