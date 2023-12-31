SELECT 
    this_week, 
    -uniq(user_id) as num_users, 
    status 
FROM
    (
    SELECT 
        user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        addWeeks(arrayJoin(weeks_visited), +1) this_week, 
        if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status
    FROM simulator_20230620.feed_actions
    group by user_id
    )
where status = 'gone'
group by this_week, status
HAVING this_week != addWeeks(toMonday(today()), +1)

union all

SELECT 
    this_week, 
    uniq(user_id) as num_users, 
    status 
FROM
    (
    SELECT
        user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        arrayJoin(weeks_visited) this_week, 
        if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status
    FROM simulator_20230620.feed_actions
    group by user_id
    )
group by this_week, status 