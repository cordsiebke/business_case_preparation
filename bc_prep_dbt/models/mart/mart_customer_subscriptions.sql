with subscriptions as (
		select
		customer_id,
		min(start_date) as start_date,
		max(end_date) as end_date
		from {{ ref('stage_subscriptions') }}
		where customer_id is not null
		group by 1
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