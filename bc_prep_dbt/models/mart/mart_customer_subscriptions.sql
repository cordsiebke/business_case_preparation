with subscriptions as (
		select
		*
		from {{ ref('stage_customer_subscriptions') }}
		)
,
customers as (
		select
		*
		from {{ ref('stage_customers') }}
		)
select
c.*,
s.start_date,
s.end_date
from subscriptions s
join customers c 
on s.customer_id = c.id