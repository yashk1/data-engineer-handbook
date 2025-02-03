-- analytical patterns day 1 lab
-- CREATE TABLE users_growth_accounting (
-- 	user_id TEXT,
-- 	first_active_date DATE,
-- 	last_active_date DATE,
-- 	daily_active_state TEXT,
-- 	weekly_active_state TEXT,
-- 	dates_active DATE[],
-- 	date DATE,
-- 	PRIMARY KEY (user_id, date)
-- );

-- select min(event_time::timestamp) , max(event_time::timestamp) from events;
-- select * from events

select * from users_growth_accounting
	where date = Date('2023-01-03')

INSERT INTO users_growth_accounting
with yesterday as(
	select * from users_growth_accounting
	where date = DATE('2023-01-02')
),
today as(
	select 
		user_id::text,
		date(date_trunc('day', event_time::TIMESTAMP)) as today_date,
		count(1) 
	from events
	where date_trunc('day', event_time::TIMESTAMP) = DATE('2023-01-03') 
		and user_id is not null
	group by user_id, 2
)


select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(y.first_active_date, t.today_date) as first_active_date,
	coalesce(t.today_date, y.last_active_date) as last_active_date,
	case 
		when y.user_id is null and t.user_id is not null then 'New'
		when y.last_active_date = t.today_date - INTERVAL '1 day' then 'Retained'
		when y.last_active_date < t.today_date - INTERVAL '1 day' then 'Resurrected' 
		when t.today_date is null and y.last_active_date = y.date then'Churned'
		else 'stale'
	end as daily_active_state,
	case 
		when y.user_id is null and t.user_id is not null then 'New'
		when y.last_active_date < t.today_date - INTERVAL '7 day' then 'Resurrected' 
		when y.last_active_date >= y.date - INTERVAL '7 day' then 'Retained'
		when t.today_date is null and y.last_active_date = y.date - INTERVAL '7 day' then 'Churned'
		else 'Stale'
	end as weekly_active_state,
	coalesce(y.dates_active, array[]::date[]) || case when t.user_id is not null then array[t.today_date] else array[]::date[] end as date_active,
	coalesce(t.today_date, y.date + INTERVAL '1 day')::date as date
from today t
	full outer join yesterday y on t.user_id = y.user_id


---
select 
	date - first_active_date as days_since_first_active,
	count(
		case when daily_active_state in ('New', 'Retained','Resurrected') then 1 end
	) as number_active,
	count(1),
	count(
		case when daily_active_state in ('New', 'Retained','Resurrected') then 1 end
	)::float / count(1) as perc_active
from users_growth_accounting
group by 1
order by 1