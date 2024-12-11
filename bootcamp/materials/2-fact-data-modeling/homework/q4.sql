--4.  A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

with users as(
	select *
	from user_devices_cumulated
	where date = '2023-01-31'::DATE
)
, SERIES AS (
	SELECT GENERATE_SERIES('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 DAY') AS SERIES_DATE
)
,PLACEHOLDER AS(
SELECT
	CASE 
		WHEN DEVICE_ACTIVITY_DATELIST @> ARRAY[SERIES_DATE::DATE] THEN CAST(POW(2, 32-( date - DATE(series_date))) as BIGINT)
	END AS placeholder_int_value,
	*
FROM USERS
	CROSS JOIN SERIES
ORDER BY USER_ID)

SELECT 
	USER_ID,
	SUM(PLACEHOLDER_INT_VALUE)::BIGINT::BIT(32),
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))) AS bit_count,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS bigint) AS bit(32))) > 0 AS dim_is_monthly_active
FROM PLACEHOLDER
GROUP BY USER_ID


