-- ------------------------------------------------------------
-- ------------------------------------------------------------
-- drop table players_scd;

-- CREATE TABLE players_scd(
-- 	player_name TEXT,
-- 	scoring_class scoring_class,
-- 	is_active BOOLEAN,
-- 	start_season INTEGER,
-- 	end_season INTEGER,
-- 	current_season INTEGER,
-- 	primary key(player_name, start_season)
-- );

-- INSERT INTO players_scd (
--     player_name,
--     scoring_class,
--     is_active,
--     start_season,
--     end_season,
--     current_season
-- )

-- WITH streak_started AS (
--     SELECT 
--         player_name,
--         current_season,
--         scoring_class,
--         is_active,
--         lag(scoring_class) OVER (PARTITION BY player_name ORDER BY current_season) <> scoring_class
--         OR lag(scoring_class) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL
--         OR lag(is_active) OVER (PARTITION BY player_name ORDER BY current_season) <> is_active
--         AS did_change
--     FROM players
-- 	where current_season <= 2022
-- ),
-- streak_identified AS (
--     SELECT
--         player_name,
--         scoring_class,
--         current_season,
--         is_active,
--         SUM(CASE WHEN did_change THEN 1 ELSE 0 END) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
--     FROM streak_started
-- )
-- ,
-- aggregated AS (
--     SELECT
--         player_name,
--         scoring_class,
--         is_active,
--         streak_identifier,
--         MIN(current_season) AS start_season,
--         MAX(current_season) AS end_season
--     FROM streak_identified
--     GROUP BY player_name, scoring_class, is_active, streak_identifier
-- )

-- SELECT 
--     player_name,
--     scoring_class,
--     is_active,
--     start_season,
--     end_season,
--     2022 AS current_season
-- FROM aggregated
-- -- ON CONFLICT (player_name, start_season) -- Resolve conflict using primary key
-- -- DO UPDATE
-- -- SET 
-- --     scoring_class = EXCLUDED.scoring_class,
-- --     is_active = EXCLUDED.is_active,
-- --     end_season = EXCLUDED.end_season,
-- --     current_season = EXCLUDED.current_season;
	



-- select * from players_scd
-- order by player_name, start_season;




-- 26 min forward----

-- CREATE TYPE scd_type AS (
--                     scoring_class scoring_class,
--                     is_active boolean,
--                     start_season INTEGER,
--                     end_season INTEGER
--                         )


WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2022
    AND end_season = 2022
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd
        WHERE current_season = 2022
        AND end_season < 2022
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2023
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2023 AS current_season FROM (
  SELECT *
  FROM historical_scd

  UNION ALL

  SELECT *
  FROM unchanged_records

  UNION ALL

  SELECT *
  FROM unnested_changed_records

  UNION ALL

  SELECT *
  FROM new_records
) a
order by player_name, start_season