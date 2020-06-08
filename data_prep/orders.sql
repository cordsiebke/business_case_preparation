SELECT
o.id,
o.customer_id,
case when o.migration_date is null then FALSE else TRUE end as order_migration_flag,
o.date_shipped,
o.total_amount_billed_retail_gross
from public.customer_order o 
join public.principal p 
on o.customer_id = p.id
where 1=1
and o.currency_code = 'EUR'
and p.shipping_country_code in ('AT', 'BE', 'FR', 'LU', 'NL')
and o.date_shipped is not null