-- Q2. Cumulative table generation query: Write a query that populates the actors table one year at a time.

truncate table actors;

DO $$
DECLARE
    year_start INT := 1969;
    year_end INT := 2020;
BEGIN
    FOR year_i IN year_start..year_end LOOP
		insert into actors
		with yesterday as(
			select * from actors
			where year = year_i
		)
		,today as(
			select 
				actorid,
				actor,
				year,
				array_agg(row(filmid, votes,rating, film)::films_struct) as films,
				avg(rating) as rating,
				CASE
					WHEN AVG(rating) > 8 THEN 'star'
					WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
					WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
					WHEN AVG(rating) <= 6 THEN 'bad'
					ELSE NULL
				END::quality_class AS quality_class
			from actor_films
			where year = year_i + 1
			group by actorid, actor, year
		) 
		select
			coalesce(t.actorid, y.actorid) as actorid,
			coalesce(t.actor, y.actor) as actor,
			case when t.year is null then y.year + 1
				else t.year
			end as year,
			case 
				when y.films is null then t.films
				when t.films is not null then y.films || t.films 
				else y.films
			END as films,
			COALESCE(t.quality_class, y.quality_class) as quality_class,
			CASE
				WHEN t.films IS NOT NULL then TRUE
				else FALSE
			END as is_active
		from today t
			full outer join yesterday y on t.actorid = y.actorid;
	END LOOP;
END $$;