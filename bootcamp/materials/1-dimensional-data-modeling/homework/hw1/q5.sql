-- Q5. Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table
-- CREATE TYPE actor_scd_type AS (
--                     quality_class quality_class,
--                     is_active boolean,
--                     start_year INTEGER,
--                     end_year INTEGER
-- );

with last_year_scd as(
	select * from actors_history_scd
	where end_year = 2020 and current_year = 2020)

,historical_scd as(
	select actor, quality_class, is_active, start_year, end_year
	from actors_history_scd
	where end_year < 2020 and current_year = 2020)

, this_year_data as(
	select * from actors
	where year = 2021)

,unchanged_data as(
	select 
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_year,
		ty.year as end_year
	from this_year_data ty
		join last_year_scd ly on ty.actor = ly.actor
	where ty.quality_class = ly.quality_class
		and ty.is_active = ly.is_active
)

,changed_data as(
	select 
		ty.actor,
		UNNEST(ARRAY[
			ROW(ly.quality_class, ly.is_active, ly.start_year, ly.end_year)::actor_scd_type,
			ROW(ty.quality_class, ty.is_active, ty.year, ty.year)::actor_scd_type
		]) as records
	from this_year_data ty
		left join last_year_scd ly on ty.actor = ly.actor
	where (ty.quality_class <> ly.quality_class) or (ty.is_active <> ly.is_active)
)

,unnested_changed_data as(
	select 
		actor,
		(records::actor_scd_type).quality_class,
		(records::actor_scd_type).is_active,
		(records::actor_scd_type).start_year,
		(records::actor_scd_type).end_year
	from changed_data
)

, new_records as(
select 
	ty.actor,
	ty.quality_class,
	ty.is_active,
	ty.year as start_year,
	ty.year as end_year
from this_year_data ty
	left join last_year_scd ly on ty.actor = ly.actor
where ly.actor is null
)

select *, 2021 as current_year from(
select * from historical_scd
union all
select * from unchanged_data
union all 
select * from unnested_changed_data
union all 
select * from new_records
)a
order by actor, start_year
