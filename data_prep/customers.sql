SELECT
o.customer_id as id,
p.shipping_country_code as country,
case when p.migration_date is null then FALSE else TRUE end as customer_migration_flag,
p.migration_merge_flag as customer_merge_flag,
min(o.date_shipped) as date_acquired
from public.customer_order o
join public.principal p 
on o.customer_id = p.id
and o.currency_code = 'EUR'
and p.shipping_country_code in ('AT', 'BE', 'FR', 'LU', 'NL')
and o.date_shipped is not null
group by 1,2,3,4