-- A monthly, reduced fact table DDL host_activity_reduced
-- month
-- host
-- hit_array - think COUNT(1)
-- unique_visitors array - think COUNT(DISTINCT user_id)

-- CREATE TABLE IF NOT EXISTS host_activity_reduced (
-- 	host TEXT,
-- 	month_start DATE,
-- 	hits REAL[],
-- 	unique_visitors REAL[],
-- 	PRIMARY KEY(HOST, MONTH_START)
-- )

INSERT INTO HOST_ACTIVITY_REDUCED
with yesterday as(
	select * 
	from host_activity_reduced
	where month_start = '2023-01-01'
)

,daily_aggregate as(
	select 
		DATE(event_time) as date,
		host, 
		count(1) as daily_hits,
		count(distinct user_id) as daily_users
	from events 
	where user_id is not null
		and date(event_time) = '2023-01-10'
	group by 1,2
)


select 
	coalesce(t.host, y.host) as host,
	coalesce(y.month_start, date_trunc('month', date) ) as month_start,
	case
		when y.hits is not null then y.hits || array[coalesce(t.daily_hits,0)]
		when y.hits is null then array_fill(0, array[ coalesce(date - DATE(date_trunc('month', t.date)), 0)] ) || array[coalesce(t.daily_hits,0)]
	end as hits,
	case
		when y.unique_visitors is not null then y.unique_visitors || array[coalesce(t.daily_users,0)]
		when y.unique_visitors is null then array_fill(0, array[ coalesce(date - DATE(date_trunc('month', t.date)), 0)] ) || array[coalesce(t.daily_users,0)]
	end as unique_visitors
from daily_aggregate t
	full outer join yesterday y on y.host = t.host
ON CONFLICT (host, month_start)
DO
	UPDATE SET unique_visitors = excluded.unique_visitors,
		hits = excluded.hits

