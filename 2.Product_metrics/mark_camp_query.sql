SELECT 
	user_id,
	source,
	toDate(time) date, 
	start_date
FROM simulator_20230620.feed_actions
join 
	(
	SELECT 
		user_id, 
		min(toDate(time)) start_date
	FROM simulator_20230620.feed_actions 
	group by user_id
	) t
using user_id 
where start_date = toDate('2023-06-13')

