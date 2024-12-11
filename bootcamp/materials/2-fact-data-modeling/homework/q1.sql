-- q1. A query to deduplicate game_details from Day 1 so there's no duplicates

with deduped as(
select 
	g.game_date_est,
	g.season,
	g.home_team_id,
	gd.*,
	row_number() over(partition by g.game_id, team_id, player_id) as rn
from game_details gd
	join games g on gd.game_id = g.game_id)

SELECT
	game_date_est as dim_game_date,
	season as dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    home_team_id = team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE rn = 1;