with cte as (
select
	*,
	DATEDIFF(case when t.to_date = date('9999-01-01') then curdate() else t.to_date end, t.from_date) as days_per_position
from
	titles t 
)
select
	e.emp_no ,
	e.first_name,
	e.last_name,
	count(distinct t.title) as total_titles,
	avg(days_per_position)/ 365 as avg_years_per_position
from
	employees e
left join cte t
on
	e.emp_no = t.emp_no
group by
	1,
	2,
	3
order by
	4 desc