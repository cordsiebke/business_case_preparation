SELECT
s.customer_id,
min(s.start_date) as start_date,
max(s.end_date) as end_date
from dw.fact_opportunity_subscription_main s 
group by 1