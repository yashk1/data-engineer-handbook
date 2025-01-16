
INSERT INTO USER_DEVICES_CUMULATED
with cte as(
	select *, row_number() over(partition by device_id) as rn
	from devices
),

deduped_devices as(
	select device_id, browser_type, browser_version_major, browser_version_patch, device_type, device_version_major, device_version_minor, device_version_patch, os_type, os_version_major, os_version_minor, os_version_patch
	from cte
	where rn =1
)

,yesterday as(
	select * 
	from user_devices_cumulated
	where date = '2022-12-31'
)

,today as(
	select user_id, d.browser_type, event_time::TIMESTAMP::date as date_active
	from events e
		join deduped_devices d on d.device_id = e.device_id
	where date(CAST(e.event_time AS TIMESTAMP)) = '2023-01-01'::DATE
		and user_id IS NOT NULL
	group by 1,2,3
)

select 
	COALESCE(t.user_id, y.user_id) as user_id,
	COALESCE(t.browser_type, y.browser_type) as broswer_type,
	CASE 
		WHEN y.device_activity_datelist is NULL then ARRAY[t.date_active]
		when t.date_active is null then y.device_activity_datelist
		else ARRAY[t.date_active] ||  y.device_activity_datelist
	END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1DAY')::DATE as date
from today t
	full outer join yesterday y on t.user_id = y.user_id and t.browser_type = y.browser_type