SELECT
o.customer_id as id,
p.shipping_country_code as country,
min(date(o.date_shipped)) as date_acquired
from public.customer_order o
join public.principal p 
on o.customer_id = p.id
and o.currency_code = 'EUR'
and p.shipping_country_code in ('AT', 'DE')
and o.date_shipped is not null
and o.migration_date is null 
group by 1,2