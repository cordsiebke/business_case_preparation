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
select
c.id as customer_id,
c.country,
c.date_acquired,
o.id as order_id,
o.date_shipped,
o.gross_basket
from orders o
join customers c 
on o.customer_id = c.id
