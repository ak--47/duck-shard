SELECT
	COUNT(*) AS total_rows,
	COUNT(DISTINCT user_id) AS unique_users,
	COUNT(DISTINCT event) AS unique_events
FROM
	input_data
