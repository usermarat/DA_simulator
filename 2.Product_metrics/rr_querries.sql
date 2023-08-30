# Retention rate по дням (на 10ый день)

SELECT  start_date,
        day,
        source, 
        round(100 * end_count.user / start_count.user, 2) retention
FROM 
    (
    SELECT  start_date, 
            toDate(time) day,
            source,
            count(distinct user_id) user
    FROM simulator_20230620.feed_actions
    join 
        (
        SELECT user_id, min(toDate(time)) start_date
        from simulator_20230620.feed_actions 
        group by user_id
        ) t0
    using user_id 
    group by start_date, day, source
    having day = start_date + 10  
    ) end_count

join 

    (
    SELECT  start_date, 
            source,
            count(distinct user_id) user
    FROM simulator_20230620.feed_actions
    join 
        (
        SELECT user_id, min(toDate(time)) start_date
        from simulator_20230620.feed_actions 
        group by user_id
        ) t1
    using user_id 
    group by start_date, source
    ) start_count
using start_date, source;

# Retention rate по неделям (за 2ую неделю)

SELECT 
    start_week,
    week,
    source, 
    round(100 * end_count.user / start_count.user, 2) retention
FROM 
    (
    SELECT
        start_week,
        toMonday(toDate(time)) week,
        source,
        count(distinct user_id) user
    FROM simulator_20230620.feed_actions
    join 
        (
        SELECT 
            user_id,
            toMonday(min(toDate(time))) start_week
        from simulator_20230620.feed_actions 
        group by user_id
        ) t0
    using user_id 
    group by start_week, week, source
    having week = addWeeks(start_week, 1)
    ) end_count
join 
    (
    SELECT
        start_week, 
        source, 
        count(distinct user_id) user
    FROM simulator_20230620.feed_actions
    join 
        (
        SELECT user_id, toMonday(min(toDate(time))) start_week
        from simulator_20230620.feed_actions 
        group by user_id
        ) t1
    using user_id 
    group by start_week, source
    ) start_count
using start_week, source