INSERT INTO HOSTS_CUMULATED

WITH YESTERDAY AS (
	SELECT *
	FROM HOSTS_CUMULATED
	WHERE DATE = '2023-01-02'
)

,TODAY AS (
	SELECT 
		HOST,
		EVENT_TIME::DATE AS DATE_ACTIVE	
	FROM EVENTS E
	WHERE EVENT_TIME::DATE = '2023-01-03'::DATE
	GROUP BY 1,2
)

SELECT
	COALESCE(T.HOST, Y.HOST),
	COALESCE(T.DATE_ACTIVE, Y.DATE + INTERVAL '1 DAY') AS DATE,
		CASE 
		WHEN Y.HOST_ACTIVITY_DATELIST IS NULL THEN ARRAY[T.DATE_ACTIVE]
		WHEN T.DATE_ACTIVE IS NULL THEN Y.HOST_ACTIVITY_DATELIST
		ELSE ARRAY[T.DATE_ACTIVE] || Y.HOST_ACTIVITY_DATELIST
	END AS HOST_ACTIVITY_DATELIST
FROM TODAY T
	FULL OUTER JOIN YESTERDAY Y ON T.HOST = Y.HOST
