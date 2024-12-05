-- Q4. Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO actors_history_scd (
	actor, 
	quality_class,
	is_active,
	start_year,
	end_year,
	current_year
)

with streak_started as(
select
 	actor, 
	quality_class,
	is_active,
	year,
	lag(quality_class) over(partition by actor order by year) <> quality_class
		or lag(quality_class) over(partition by actor order by year) IS NULL
		or lag(is_active) over(partition by actor order by year) <> is_active
	as change
from actors
where year <= 2020)

, streak_identified as(
select 
	actor,
	quality_class,
	is_active, 
	year,
	sum(case when change then 1 else 0 end) over(partition by actor order by year) as streak
from streak_started)

,aggregated as(
select 
	actor,
	quality_class,
	is_active,
	streak,
	min(year) as start_year,
	max(year) as end_year
from streak_identified
group by actor, quality_class, is_active, streak)

select
	actor,
	quality_class,
	is_active,
	start_year,
	end_year,
	'2020' as current_year
from aggregated
order by actor, start_year
