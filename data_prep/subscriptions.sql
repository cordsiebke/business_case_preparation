SELECT
s.id,
s.customer_id,
s.start_date as start_date,
s.end_date as end_date
from dw.fact_opportunity_subscription_main s 
