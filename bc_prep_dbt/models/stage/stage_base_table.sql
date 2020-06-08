with prep as (
			select 
			date_trunc('month', "date") as month,
			1 as match
			from {{ ref('stage_calendar') }}
			group by 1,2
			),
dimension_1 as (
			select
			country,
			1 as match	
			from {{ ref('stage_customers')}}
			group by 1,2
			)
select 
date(p1.month) as cohort_month,
date(p2.month) as action_month,
extract(year from age(p2.month,p1.month))*12 + extract(month from age(p2.month,p1.month)) as lifetime_month,
d1.country
from prep p1
join prep p2
on p1.match=p2.match
join dimension_1 d1
on d1.match = p1.match
where p1.month <= p2.month
order by 1,2,3