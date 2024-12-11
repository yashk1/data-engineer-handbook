-- week 2 lab 3

-- create table array_metrics(
-- 	user_id NUMERIC,
-- 	month_start DATE,
-- 	metric_name TEXT,
-- 	metric_array REAL[],
-- 	primary key(user_id, month_start, metric_name)
-- );

insert into array_metrics
with daily_aggregate as(
	select
		user_id,
		DATE(event_time) as date,
		count(1) as num_site_hits
	from events
	where DATE(event_time) = DATE('2023-01-02')
		and user_id IS NOT NULL
	group by 1, 2
),

yesterday_array as(
	select * from array_metrics
	where month_start = DATE('2023-01-01')
)


select 
	coalesce(da.user_id, y.user_id) as user_id,
	coalesce(y.month_start, DATE_TRUNC('month', da.date)) as month_start,
	'site_hits' as metric_name,
	case
		when y.metric_array is not null then y.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
		when y.metric_array is null then ARRAY_FILL(0, ARRAY[ coalesce(date - DATE(DATE_TRUNC('month', da.date)) ,0) ]) || ARRAY[COALESCE(da.num_site_hits,0)] -- imp
	end as metric_array
from daily_aggregate da
	full outer join yesterday_array y on da.user_id = y.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
	UPDATE SET metric_array = EXCLUDED.metric_array;


select cardinality(metric_array), count(1)
from array_metrics
group by 1;


with monthly_agg as(
	select 
		metric_name,
		month_start,
		array[ sum(metric_array[1]) , sum(metric_array[2]) ]  as summed_array
	from array_metrics
	group by metric_name, month_start
)

select metric_name, month_start + CAST(CAST(index - 1 as TEXT) || 'day' as INTERVAL),
	elem as value
from monthly_agg
	cross join unnest(monthly_agg.summed_array) 
		with ordinality as a(elem, index)