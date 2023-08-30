SELECT 
    city,
    uniqIf(user_id, toDate(time) = toDate('2023-06-21')) day_21,
    uniqIf(user_id, toDate(time) = toDate('2023-06-22')) day_22,
    day_21 - day_22 delta
FROM simulator_20230620.feed_actions
where toDate(time) in (toDate('2023-06-21'), toDate('2023-06-22'))
and country = 'Russia'
group by city
order by delta desc