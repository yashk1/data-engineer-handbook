with users as(
	select * from users_cumulated
	where date = DATE('2023-01-31')
)

,series as(
	select * from generate_series(DATE('2023-01-01'), DATE('2023-01-31'),INTERVAL '1 day') as series_date
)
, placeholder_ints as(
	select 
		case 
			when date_active @> ARRAY[DATE(series_date)] then CAST(POW(2, 32-( date - DATE(series_date))) as BIGINT) 
			else 0
		end  as placeholder_int_value,
		*
	from users u 
		cross join series 
	order by user_id
)

SELECT 
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32)) AS bit_sum,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))) AS bit_count,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))) > 0 AS dim_is_monthly_active,
    BIT_COUNT(
        CAST('11111110000000000000000000000000' AS bit(32))
        & CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))
    ) > 0 AS dim_is_weekly_active
FROM placeholder_ints
GROUP BY user_id
ORDER BY bit_count DESC;





