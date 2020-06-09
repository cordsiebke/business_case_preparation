SELECT
o.id,
o.customer_id,
date(o.date_shipped) as date_shipped,
o.total_amount_billed_retail_gross as gross_basket
from public.customer_order o 
join public.principal p 
on o.customer_id = p.id
where 1=1
and o.currency_code = 'EUR'
and p.shipping_country_code in ('AT', 'DE')
and o.date_shipped is not null
and o.migration_date is null 