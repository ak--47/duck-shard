SELECT event, user_id, CAST(time AS VARCHAR) AS time_str FROM input_data WHERE event IS NOT NULL;
