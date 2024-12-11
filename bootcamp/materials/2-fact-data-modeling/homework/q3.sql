insert into user_devices_cumulated

with yesterday as(
	select *
	from user_devices_cumulated
	where date = '2023-01-01'::DATE
)

, today as(
	SELECT 
		e.user_id,
		d.browser_type,
		e.event_time::TIMESTAMP::DATE as date_active
	FROM events e
		join devices d 
			on e.device_id = d.device_id
	where date(CAST(e.event_time AS TIMESTAMP)) = '2023-01-02'::DATE
		and user_id is not null
	group by 1,2,3
)
select 
	COALESCE(t.user_id, y.user_id) as user_id,
	COALESCE(t.browser_type, y.browser_type) as browser_type,
	case
		when y.device_activity_datelist is null then ARRAY[t.date_active]
		when t.date_active is null then y.device_activity_datelist
		else ARRAY[t.date_active] || y.device_activity_datelist
	end as device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day')::DATE as date
from today t
	full outer join yesterday y
		on t.user_id = y.user_id 
		and t.browser_type = y.browser_type;


select * from user_devices_cumulated;