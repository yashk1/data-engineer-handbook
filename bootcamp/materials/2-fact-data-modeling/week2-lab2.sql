-- -- week 2 lab 2
-- select * from events;

-- select min(event_time), max(event_time) from events;


-- CREATE TABLE users_cumulated(
-- 	user_id TEXT,
-- 	date_active DATE[], --list of dates in the past where the user was active
-- 	date DATE, -- the current date for the user
-- 	PRIMARY KEY(user_id, date)
-- );

INSERT INTO users_cumulated
with yesterday as(
	select *
	from users_cumulated
	where date = DATE('2023-01-06')
)

,today as(
	select 
		cast(user_id as TEXT) as user_id,
		DATE(CAST(event_time AS TIMESTAMP)) as date_active
	from events
	where DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-07')
		and user_id IS NOT NULL
	group by 1,2
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	CASE WHEN y.date_active is null then ARRAY[t.date_active]
		 WHEN t.date_active is null then y.date_active
		 ELSE ARRAY[t.date_active] || y.date_active 
	END,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') as date
from today t
	full outer join yesterday y on y.user_id = t.user_id;



-- select * from users_cumulated
-- where date = DATE('2023-01-02')
