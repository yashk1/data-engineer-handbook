-- A monthly, reduced fact table DDL host_activity_reduced
-- month
-- host
-- hit_array - think COUNT(1)
-- unique_visitors array - think COUNT(DISTINCT user_id)

CREATE TABLE IF NOT EXISTS host_activity_reduced (
	host TEXT,
	month_start DATE,
	hits REAL[],
	unique_visitors REAL[],
	PRIMARY KEY(HOST, MONTH_START)
)
