 -- Analytical Patterns - Lab 2


SELECT url, COUNT(1) 
FROM events
-- where url like '%/api%'
GROUP BY url
ORDER BY 2 DESC;


WITH deduped_events AS (
    SELECT
        url, user_id, event_time,  event_time::date as event_date
    FROM events
	where user_id is not null
		and url in ('/signup', '/login')
    GROUP BY 1,2,3,4
),
selfjoined as(
	select  d1.user_id, d1.url, d2.url as destination_url, d1.event_time, d2.event_time
	from deduped_events d1 
	join deduped_events d2 on d1.user_id = d2.user_id
		and d1.event_date = d2.event_date 
		and d1.event_time < d2.event_time
	where d1.url = '/signup' 
),
userlevel as(
	select 
		user_id,
		max(case when destination_url = '/login' then 1 else 0 end) as converted
	from selfjoined
	group by 1
)
select 
	count(1),
	sum(converted)
from userlevel


--- url level
WITH deduped_events AS (
    SELECT
        url, user_id, event_time,  event_time::date as event_date
    FROM events
	where user_id is not null
		-- and url in ('/signup', '/login')
    GROUP BY 1,2,3,4
),
selfjoined as(
	select  d1.user_id, d1.url, d2.url as destination_url, d1.event_time, d2.event_time
	from deduped_events d1 
	join deduped_events d2 on d1.user_id = d2.user_id
		and d1.event_date = d2.event_date 
		and d1.event_time < d2.event_time
	-- where d1.url = '/signup' 
),
userlevel as(
	select 
		user_id,
		url,
		count(1) as num_hits,
		sum(case when destination_url = '/login' then 1 else 0 end) as converted
	from selfjoined
	group by 1, 2
)
select 
	url,
	sum(num_hits) as num_hits,
	sum(converted) as num_converted,
	round(sum(converted) / sum(num_hits) ,2) as pct_convert
from userlevel
group by 1
order by 4 desc




------------------------------
------------------------------

CREATE TABLE device_hits_dashboard as

with events_augmented as (
	select 
		coalesce(d.os_type,'unknown') as os_type,
		coalesce(d.device_type,'unknown') as device_type,
		coalesce(d.browser_type,'unknown') as browser_type,
		url,
		user_id
	from events e
		join devices d on e.device_id = d.device_id
)
select
	case 
		when grouping(os_type) = 0 then 'os_type' 
		when grouping(device_type) = 0 then 'device_type'
		when grouping(browser_type) = 0 then 'browser_type'
		when grouping(os_type) = 0 
			and grouping(device_type) = 0 
			and grouping(browser_type) = 0 
			then 'os_type__device_type__browser_type'
	end as aggregation_level,
	coalesce(os_type ,'(overall)') as os_type,
	coalesce(device_type ,'(overall)') as device_type,
	coalesce(browser_type ,'(overall)') as browser_type,
	count(1) as num_hits
from events_augmented
group by grouping sets(
	(os_type, device_type, browser_type),
	(os_type),
	(device_type),
	(browser_type)
)
order by count(1) desc;



select * from device_hits_dashboard
where aggregation_level = 'device_type'
