-- CREATE TABLE fct_game_details(
-- 	dim_game_date DATE,
-- 	dim_season INTEGER,
-- 	dim_team_id INTEGER,
-- 	dim_player_id INTEGER,
-- 	dim_player_name TEXT,
-- 	dim_start_position TEXT,
-- 	dim_is_playing_at_home BOOLEAN,
-- 	dim_did_not_play BOOLEAN,
-- 	dim_did_not_dress BOOLEAN,
-- 	dim_not_with_team BOOLEAN,
-- 	m_minutes REAL,
-- 	m_fgm INTEGER,
-- 	m_fga INTEGER,
-- 	m_fg3m INTEGER,
-- 	m_fg3a INTEGER,
-- 	m_ftm INTEGER,
-- 	m_fta INTEGER,
-- 	m_oreb INTEGER,
-- 	m_dreb INTEGER,
-- 	m_reb INTEGER,
-- 	m_ast INTEGER,
-- 	m_blk INTEGER,
-- 	m_turnovers INTEGER,
-- 	m_pf INTEGER,
-- 	m_pts INTEGER,
-- 	m_plus_minus INTEGER,
-- 	PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
-- );

-- INSERT INTO fct_game_details 
-- with deduped as(
-- select 
-- 	g.game_date_est,
-- 	g.season,
-- 	g.home_team_id,
-- 	gd.*,
-- 	row_number() over(partition by g.game_id, team_id, player_id) as rn
-- from game_details gd
-- 	join games g on gd.game_id = g.game_id)

-- SELECT
-- 	game_date_est as dim_game_date,
-- 	season as dim_season,
--     team_id AS dim_team_id,
--     player_id AS dim_player_id,
--     player_name AS dim_player_name,
--     start_position AS dim_start_position,
--     home_team_id = team_id AS dim_is_playing_at_home,
--     COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
--     COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
--     COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
--     CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
--     fgm AS m_fgm,
--     fga AS m_fga,
--     fg3m AS m_fg3m,
--     fg3a AS m_fg3a,
--     ftm AS m_ftm,
--     fta AS m_fta,
--     oreb AS m_oreb,
--     dreb AS m_dreb,
--     reb AS m_reb,
--     ast AS m_ast,
--     blk AS m_blk,
--     "TO" AS m_turnovers,
--     pf AS m_pf,
--     pts AS m_pts,
--     plus_minus AS m_plus_minus
-- FROM deduped
-- WHERE rn = 1;


-- player who didnt did_not_with_team
select 
	dim_player_name,
	count(1) as num_games,
	count(case when dim_not_with_team then 1 end) as bailed_out,
	CAST(count(case when dim_not_with_team then 1 end)as REAL) / count(1) *100 as bailed_out_prec
from fct_game_details
group by 1
order by 4 desc

