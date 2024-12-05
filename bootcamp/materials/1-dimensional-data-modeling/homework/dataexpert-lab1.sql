-- select * from player_seasons
-- order by player_name;

-- CREATE TYPE SEASON_STATS AS (
-- 	SEASON INTEGER,
-- 	GP INTEGER,
-- 	pts REAL,
-- 	reb REAL,
-- 	ast REAL
-- );

-- CREATE TYPE scoring_class as ENUM ('star','good','average','bad')


CREATE TABLE PLAYERS (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	is_active BOOLEAN,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

INSERT INTO PLAYERS
with yesterday as(
	select * from players
	where current_season = 2003
),
today as(
	select * from player_seasons
	where season=2004
)


select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null 
		then ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
		)::season_stats]
		when t.season IS NOT NULL then y.season_stats || ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
		)::season_stats] 
		else y.season_stats 
		end as season_stats,
	CASE 
		WHEN T.SEASON IS NOT NULL THEN
			CASE
				WHEN t.pts > 20 THEN 'star'
				WHEN t.pts >15 THEN 'good'
				WHEN t.pts >10 THEN 'average'
				else 'bad'
			end::scoring_class
		ELSE y.scoring_class
	END as scoring_class,
	
	CASE WHEN T.SEASON IS NOT NULL THEN 0
		ELSE y.years_since_last_season + 1
	end as years_since_last_season,

	t.season IS NOT NULL as is_active,
	
	COALESCE(t.season, y.current_season+1) AS current_season
from today t FULL OUTER JOIN yesterday y
	on t.player_name = y.player_name;


-- with unnested as(
-- select 
-- 	player_name,
-- 	unnest (season_stats ) as season_stats 
-- from players 
-- where current_season = 2001
-- and player_name = 'Michael Jordan')

-- select 
-- 	player_name,
-- 	(season_stats::season_stats).*
-- from unnested

-- select * from players
-- where current_season = 2001
-- and player_name = 'Michael Jordan';

-- -- players who had most importatn from their first season to most recent season
-- select 
-- 	player_name,
-- 	(season_stats[1]::season_stats).pts as first_season,
-- 	(season_stats[cardinality(season_stats)]::season_stats).pts as latest_season,
-- 	(season_stats[cardinality(season_stats)]::season_stats).pts / 
-- 	case
-- 		when (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts
-- 	end
-- from players
-- where current_season = 2001 
-- order by 4 desc

drop table players;

CREATE TABLE PLAYERS (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	is_active BOOLEAN,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

DO $$
DECLARE
    year_start INT := 1996;
    year_end INT := 2022;
BEGIN
    FOR year IN year_start..year_end LOOP
        INSERT INTO PLAYERS
        WITH yesterday AS (
            SELECT * 
            FROM players
            WHERE current_season = year
        ),
        today AS (
            SELECT * 
            FROM player_seasons
            WHERE season = year + 1
        )
        SELECT 
            COALESCE(t.player_name, y.player_name) AS player_name,
            COALESCE(t.height, y.height) AS height,
            COALESCE(t.college, y.college) AS college,
            COALESCE(t.country, y.country) AS country,
            COALESCE(t.draft_year, y.draft_year) AS draft_year,
            COALESCE(t.draft_round, y.draft_round) AS draft_round,
            COALESCE(t.draft_number, y.draft_number) AS draft_number,
            CASE 
                WHEN y.season_stats IS NULL THEN 
                    ARRAY[ROW(
                        t.season,
                        t.gp,
                        t.pts,
                        t.reb,
                        t.ast
                    )::season_stats]
                WHEN t.season IS NOT NULL THEN 
                    y.season_stats || ARRAY[ROW(
                        t.season,
                        t.gp,
                        t.pts,
                        t.reb,
                        t.ast
                    )::season_stats]
                ELSE 
                    y.season_stats 
            END AS season_stats,
            CASE 
                WHEN t.season IS NOT NULL THEN
                    CASE 
                        WHEN t.pts > 20 THEN 'star'
                        WHEN t.pts > 15 THEN 'good'
                        WHEN t.pts > 10 THEN 'average'
                        ELSE 'bad'
                    END::scoring_class
                ELSE 
                    y.scoring_class
            END AS scoring_class,
            CASE 
                WHEN t.season IS NOT NULL THEN 0
                ELSE y.years_since_last_season + 1
            END AS years_since_last_season,
            t.season IS NOT NULL AS is_active,
            COALESCE(t.season, y.current_season + 1) AS current_season
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.player_name = y.player_name;
    END LOOP;
END $$;

select current_season, count(player_name) from players
group by 1
order by 1;
