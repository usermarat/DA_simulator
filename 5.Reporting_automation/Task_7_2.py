import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib_venn import venn2
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import time


# Функция загрузки данных из CH
def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

# Функция отправки ботом
def send_bot(chat=None, message=None, plot_object=None):
    chat_id = chat or 377590876
    my_token = '632525***vko'
    bot = telegram.Bot(token=my_token)
    if message:
        bot.sendMessage(chat_id=chat_id, text=message)
    if plot_object:
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
# часто используемые переменные
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230620',
              'user':'student',
              'password':'dpo_python_2020'
             }
date = datetime.date(datetime.today() - timedelta(days=1))

## блок с запросами в Кликхаус

# DAU по обоим сервисам приложения
query_DAU = '''
                SELECT 
                    day,
                    'feed&message' as service,
                    count(distinct user_id) DAU
                FROM
                (
                    SELECT 
                        toDate(time) as day,
                        user_id
                    FROM simulator_20230620.feed_actions
                    join 
                    (
                        SELECT 
                            toDate(time) as day, 
                            user_id 
                        FROM simulator_20230620.message_actions
                    ) t0
                    using (day, user_id)
                ) t1
                where 
                day between today() - 7 and today() - 1
                group by day
                union all
                SELECT 
                    toDate(time) as day,
                    'feed' as service,
                    count(distinct user_id) as DAU
                FROM simulator_20230620.feed_actions 
                where
                day between today() - 7 and today() - 1
                group by day
                union all
                SELECT 
                    toDate(time) as day,
                    'message',
                    count(distinct user_id)
                FROM 
                simulator_20230620.message_actions 
                where 
                day between today() - 7 and today() - 1
                group by day
            '''

# количество всех пользователей всего и новых за день
query_user = '''
                SELECT 
                    'feed' service,
                    count(distinct user_id) total_user,
                    (select count(*) from
                                        (
                                        SELECT
                                            user_id, 
                                            min(toDate(time)) start_date
                                            from simulator_20230620.feed_actions
                                            group by user_id
                                            having start_date = yesterday()
                                        )
                    ) new_user                    
                FROM 
                simulator_20230620.feed_actions 
                where toDate(time) <= yesterday()
                union all
                SELECT 
                    'message' service,
                    count(distinct user_id) total_user,
                    (select count(*) from
                                        (
                                        SELECT
                                            user_id, 
                                            min(toDate(time)) start_date
                                            from simulator_20230620.message_actions
                                            group by user_id
                                            having start_date = yesterday()
                                        )
                    ) new_user 
                FROM 
                simulator_20230620.message_actions 
                where toDate(time) <= yesterday()
            '''

# количество всех уникальных постов
query_total_post =  '''
                        SELECT 
                            count(distinct post_id) total_post
                        FROM 
                        simulator_20230620.feed_actions 
                        where toDate(time) <= yesterday()
                    '''

# показатели по постам в динамике за последние 7 дней
query_content = '''
                    SELECT * from
                    (
                        SELECT 
                            toDate(time) day,
                            count(distinct post_id) total_post,
                            countIf(action='view') views,
                            countIf(action='like') likes,
                            round(views / total_post) avg_views,
                            round(100 * avg_views / count(distinct user_id), 2) avg_dau_share,
                            round(100 * likes / views, 2) CTR
                        FROM 
                        simulator_20230620.feed_actions 
                        where 
                        day between yesterday() - 6 and yesterday()
                        group by day
                    ) t0
                    join
                    (
                        SELECT 
                            toDate(time) day,
                            count(distinct post_id) new_post
                        FROM 
                        simulator_20230620.feed_actions 
                        join 
                        (
                            SELECT 
                                post_id,
                                min(toDate(time)) public_day
                            FROM 
                            simulator_20230620.feed_actions 
                            where 
                            toDate(time) between yesterday() - 7 and yesterday()
                            group by post_id
                        ) t
                        using post_id
                        where public_day = day
                        and day between yesterday() - 6 and yesterday()
                        group by day
                    ) t1
                    using day
                    '''

# подсчет всех действий в приложении за последние 7 дней
query_action = '''
                    SELECT
                        day,
                        views,
                        likes,
                        messages,
                        average_view,
                        average_like,
                        average_mess
                    FROM
                    (
                        SELECT
                            toDate(time) day,
                            count(distinct user_id) DAU,
                            countIf(action='view') views,
                            countIf(action='like') likes,
                            round(views / DAU, 2) average_view,
                            round(likes / DAU, 2) average_like
                        FROM simulator_20230620.feed_actions 
                        where toDate(time) between today() - 7 and today() - 1
                        group by day
                    ) t1
                    join 
                    (
                        SELECT 
                            toDate(time) day, 
                            count(distinct user_id) DAU,
                            count(*) messages,
                            round(messages/ DAU, 2) average_mess
                        FROM simulator_20230620.message_actions 
                        where toDate(time) between today() - 7 and today() - 1
                        group by day
                    ) t2
                    using day
                '''

# second day retention по сервисам приложения в разрезе трафика
query_rr_source = '''
                    SELECT 
                        day,
                        source,
                        t_feed.retention RR_feed,
                        t_mess.retention RR_mess
                    FROM
                    (
                        SELECT
                            day,
                            source,
                            round(100 * end_count.user / start_count.user, 2) retention
                        FROM
                        (
                            SELECT 
                                start_date,
                                toDate(time) day,
                                source,
                                count(distinct user_id) user
                            FROM simulator_20230620.feed_actions
                            join
                            (
                                SELECT 
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.feed_actions
                                group by user_id
                            ) t0
                            using user_id
                            group by 
                                start_date,
                                day,
                                source
                            having day = start_date + 1
                        ) end_count
                        join
                        (
                            SELECT 
                                start_date,
                                toDate(time) day,
                                source,
                                count(distinct user_id) user
                            FROM simulator_20230620.feed_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                 FROM simulator_20230620.feed_actions
                                 group by user_id
                            ) t1 
                            using user_id
                            group by
                                start_date,
                                day,
                                source
                            having day = start_date
                        ) start_count 
                        using start_date, source
                        where day between today() - 7 and today() - 1
                    ) t_feed
                    join
                    (
                        SELECT
                            day,
                            source,
                            round(100 * end_count.user / start_count.user, 2) retention
                        FROM
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                source,
                                count(distinct user_id) user
                            FROM simulator_20230620.message_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.message_actions
                                group by user_id
                            ) t0
                            using user_id
                            group by
                                start_date,
                                day,
                                source
                            having day = start_date + 1
                        ) end_count
                        join
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                source,
                                count(distinct user_id) user
                            FROM simulator_20230620.message_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.message_actions
                                group by user_id
                            ) t1
                            using user_id
                            group by
                                start_date,
                                day,
                                source
                            having day = start_date
                        ) start_count 
                        using start_date, source
                        where day between today() - 7 and today() - 1
                    ) t_mess
                    using (day, source)
                '''

# second day retention по сервисам приложения общий
query_rr_total = '''
                    SELECT 
                        day,
                        t_feed.retention RR_feed,
                        t_mess.retention RR_mess
                    FROM
                    (
                        SELECT
                            day,
                            round(100 * end_count.user / start_count.user, 2) retention
                        FROM
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                count(distinct user_id) user
                            FROM simulator_20230620.feed_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.feed_actions
                                group by user_id
                            ) t0
                            using user_id
                            group by 
                                start_date,
                                day
                            having day = start_date + 1
                        ) end_count
                        join
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                count(distinct user_id) user
                            FROM simulator_20230620.feed_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.feed_actions
                                group by user_id
                            ) t1 
                            using user_id
                            group by
                                start_date,
                                day
                            having day = start_date
                        ) start_count
                        using start_date
                        where day between today() - 7 and today() - 1
                    ) t_feed
                    join
                    (
                        SELECT
                            day,
                            round(100 * end_count.user / start_count.user, 2) retention
                        FROM
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                count(distinct user_id) user
                            FROM simulator_20230620.message_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.message_actions
                                group by user_id
                            ) t0 
                            using user_id
                            group by 
                                start_date,
                                day
                            having day = start_date + 1
                        ) end_count
                        join
                        (
                            SELECT
                                start_date,
                                toDate(time) day,
                                count(distinct user_id) user
                            FROM simulator_20230620.message_actions
                            join
                            (
                                SELECT
                                    user_id,
                                    min(toDate(time)) start_date
                                FROM simulator_20230620.message_actions
                                group by user_id
                            ) t1
                            using user_id
                            group by
                                start_date,
                                day
                            having day = start_date
                        ) start_count
                        using start_date
                        where day between today() - 7 and today() - 1
                    ) t_mess
                    using day
                '''

# stickiness по сервисам приложения в разрезе трафика
query_st_source = '''
                    SELECT
                        day,
                        source,
                        t_feed.stickiness ST_feed,
                        t_mess.stickiness ST_mess
                    FROM
                    (
                        SELECT
                            day,
                            source,
                            round(100 * daily / monthly, 2) as stickiness
                        FROM
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                source,
                                count(DISTINCT user_id) AS monthly
                            FROM simulator_20230620.feed_actions
                            GROUP BY
                                month,
                                source
                        ) t1
                        JOIN
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                toStartOfDay(toDateTime(time)) AS day,
                                source,
                                count(DISTINCT user_id) AS daily
                            FROM simulator_20230620.feed_actions
                            GROUP BY
                                month,
                                day,
                                source
                        ) t2 
                        using month, source
                        where day between today() - 7 and today() - 1
                    ) t_feed
                    join
                    (
                        SELECT
                            day,
                            source,
                            round(100 * daily / monthly, 2) as stickiness
                        FROM
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                source,
                                count(DISTINCT user_id) AS monthly
                            FROM simulator_20230620.message_actions
                            GROUP BY
                                month,
                                source
                        ) t1
                        JOIN
                        (
                        SELECT
                            toStartOfMonth(toDateTime(time)) AS month,
                            toStartOfDay(toDateTime(time)) AS day,
                            source,
                            count(DISTINCT user_id) AS daily
                        FROM simulator_20230620.message_actions
                        GROUP BY
                            month,
                            day,
                            source
                        ) t2 
                        using month, source
                        where day between today() - 7 and today() - 1
                    ) t_mess
                    using (day, source)
                '''

# stickiness по сервисам приложения общий
query_st_total = '''
                    SELECT
                        day,
                        t_feed.stickiness ST_feed,
                        t_mess.stickiness ST_mess
                    FROM
                    (
                        SELECT
                            day,
                            round(100 * daily / monthly, 2) as stickiness
                        FROM
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                count(DISTINCT user_id) AS monthly
                            FROM simulator_20230620.feed_actions
                            GROUP BY month
                        ) t1
                        JOIN
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                toStartOfDay(toDateTime(time)) AS day,
                                count(DISTINCT user_id) AS daily
                            FROM simulator_20230620.feed_actions
                            GROUP BY
                                month,
                                day
                        ) t2 
                        using month
                        where day between today() - 7 and today() - 1
                    ) t_feed
                    join
                    (
                        SELECT
                            day,
                            round(100 * daily / monthly, 2) as stickiness
                        FROM
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                count(DISTINCT user_id) AS monthly
                            FROM simulator_20230620.message_actions
                            GROUP BY month
                        ) t1
                        JOIN
                        (
                            SELECT
                                toStartOfMonth(toDateTime(time)) AS month,
                                toStartOfDay(toDateTime(time)) AS day,
                                count(DISTINCT user_id) AS daily
                            FROM simulator_20230620.message_actions
                            GROUP BY
                                month,
                                day
                        ) t2
                        using month
                        where day between today() - 7 and today() - 1
                    ) t_mess
                    using day
                '''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
                'owner': 'm.ahmadeev',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 7, 12)
               }

# Интервал запуска DAG - с запасом 10 минут под исполнение и очередность
schedule_interval = '50 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task_7_2_ahmadeev():
    
    # функция выгрузки
    @task()
    def extract(query, connection):
        df = ch_get_df(query, connection)
        return df
    
    # функция отправки 1 части отчета - аудиторные метрики
    @task()
    def report_user(list_df):
        
        # данные хранятся в двух выгруженных датасетах
        df_dau, df_user = list_df
        
        # манипуляции для более удобного использования переменных
        df_dau.set_index('day', inplace=True)
        feed = df_dau.loc[date.strftime("%Y-%m-%d")].query('service == "feed"').DAU[0]
        message = df_dau.loc[date.strftime("%Y-%m-%d")].query('service == "message"').DAU[0]
        cross = df_dau.loc[date.strftime("%Y-%m-%d")].query('service == "feed&message"').DAU[0]
        
        msg = f'''Ежедневный расширенный отчет
        
Часть 1 из 4х

Аудиторные метрики по приложению на {date.strftime("%d.%m.%Y")}:

Общее количество уникальных пользователей (новостником) - {df_user.query('service == "feed"')['total_user'].values[0]}, 
    из них новые за день - {df_user.query('service == "feed"')['new_user'].values[0]} (прирост {round(100 * df_user.query('service == "feed"')['new_user'].values[0] / df_user.query('service == "feed"')['total_user'].values[0], 2)} %)
В т.ч. мессенджером - {df_user.query('service == "message"')['total_user'].values[0]} ({int(100 * df_user.query('service == "message"')['total_user'].values[0] / df_user.query('service == "feed"')['total_user'].values[0])} % от аудитории новостника), 
    из них новые за день - {df_user.query('service == "message"')['new_user'].values[0]} (прирост {round(100 * df_user.query('service == "message"')['new_user'].values[0] / df_user.query('service == "message"')['total_user'].values[0], 2)} %)

DAU по всему приложению - {feed + message - cross}, в т.ч.:
    по новостнику - {feed - cross}
    по мессенджеру - {message - cross}
    пересеченная аудитория (использовали оба сервиса) - {cross}
'''
        # график DAU
        sns.set()
        fig, (ax0, ax1) = plt.subplots(1, 2, figsize=(25, 8), gridspec_kw={'width_ratios': [1, 4]})
        venn2(subsets=(feed - cross, message - cross, cross),
              set_labels=('feed', 'message'),
              set_colors=('skyblue', 'purple'),
              ax=ax0)
        ax0.set_title(f'DAU всего приложения за {date.strftime("%d.%m.%Y")} - {feed + message - cross}')
        
        plt.stackplot(df_dau.index.unique(), 
                      df_dau.query('service == "message"').DAU - df_dau.query('service == "feed&message"').DAU,
                      df_dau.query('service == "feed&message"').DAU,
                      df_dau.query('service == "feed"').DAU - 2 * df_dau.query('service == "feed&message"').DAU,
                      alpha=0.6,
                      colors=('purple', 'yellow', 'skyblue'), 
                      labels=('message', 'feed&message', 'feed'))
        plt.plot(df_dau.query('service == "message"').DAU +
                 df_dau.query('service == "feed"').DAU - 
                 2*df_dau.query('service == "feed&message"').DAU,
                 label='total')
        plt.legend(loc='center left')
        ax1.set_title(f'Динамика DAU за период {(date - timedelta(days=6)).strftime("%d.%m.%Y")} - {date.strftime("%d.%m.%Y")}')
                
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU_report.png'
        plt.close()
        
        # отправка отчетов в чат
        send_bot(chat=-867652742, message=msg, plot_object=plot_object)
                
    # функция отправки 2 части отчета - контент метрики
    @task()
    def report_post(list_df):
        
        # данные хранятся в двух выгруженных датасетах
        df_cont, df_post = list_df
        df_cont.set_index('day', inplace=True)
        
        msg = f'''Часть 2 из 4х
        
Контент метрики по приложению на {date.strftime("%d.%m.%Y")}:

Общее количество уникальных постов - {df_post['total_post'][0]}, 
    из них новые за день - {int(df_cont.loc[date.strftime("%Y-%m-%d")].new_post)} (прирост {int(100 * df_cont.loc[date.strftime("%Y-%m-%d")].new_post / df_post['total_post'][0])} %)
Количество уникальных постов в ленте за день - {int(df_cont.loc[date.strftime("%Y-%m-%d")].total_post)}
Количество просмотров в среднем на пост за день - {int(df_cont.loc[date.strftime("%Y-%m-%d")].avg_views)}
Доля от DAU (охват) - {df_cont.loc[date.strftime("%Y-%m-%d")].avg_dau_share} %
Усредненный CTR на пост - {df_cont.loc[date.strftime("%Y-%m-%d")].CTR} %
               '''
        
        # графики по постам
        sns.set()
        fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(15, 8), sharex=True)
        sns.lineplot(data=df_cont[['total_post', 'new_post']], ax=ax0)
        ax0.set_title('Количетво уникальных постов')
        sns.lineplot(data=df_cont[['avg_dau_share', 'CTR']], ax=ax1)
        ax1.set_title('CTR и охват')
                
        # сохранение и отправка изображения с графиками
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Post_report.png'
        plt.close()
        
        # отправка отчетов в чат
        send_bot(chat=-867652742, message=msg, plot_object=plot_object)
        
    # функция отправки 3 части отчета - метрики активности
    @task()
    def report_action(df):
        
        # все в одном датафрейме
        df.set_index('day', inplace=True)
        
        msg = f'''Часть 3 из 4х
        
Метрики активности по приложению за {date.strftime("%d.%m.%Y")}:

Количество просмотров - {int(df.loc[date.strftime("%Y-%m-%d")].views)}, 
    в среднем на 1 польз. DAU - {df.loc[date.strftime("%Y-%m-%d")].average_view}
Количество лайков - {int(df.loc[date.strftime("%Y-%m-%d")].likes)}, 
    в среднем на 1 польз. DAU - {df.loc[date.strftime("%Y-%m-%d")].average_like}
Количество сообщений - {int(df.loc[date.strftime("%Y-%m-%d")].messages)}, 
    в среднем на 1 польз. DAU - {df.loc[date.strftime("%Y-%m-%d")].average_mess}
    '''
        
        # графики по действиям в приложении
        sns.set()
        fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(15, 8), sharex=True)
        sns.lineplot(data=df[['views', 'likes', 'messages']], ax=ax0)
        ax0.set_title('Общее количество действий в приложении')
        sns.lineplot(data=df[['average_view', 'average_like', 'average_mess']], ax=ax1)
        ax1.set_title('В среднем на 1 пользователя DAU')
                
        # сохранение и отправка изображения с графиками
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Action_report.png'
        plt.close()
        
        # отправка отчетов в чат
        send_bot(chat=-867652742, message=msg, plot_object=plot_object)

    # функция отправки 4 части отчета - метрики эффективности
    @task()
    def report_effect(list_df):
        
        # данные хранятся в 4 выгруженных датасетах         
        df_rr_source, df_rr_total, df_st_source, df_st_total = list_df
        
        df_rr_source.set_index('day', inplace=True)
        df_rr_total.set_index('day', inplace=True)
        df_st_source.set_index('day', inplace=True)
        df_st_total.set_index('day', inplace=True)
        
        
        msg = f'''Часть 4 из 4х
        
Метрики эффективности по приложению на {date.strftime("%d.%m.%Y")}:

Retention rate новых пользователей предыдущего дня, %:
По новостнику в целом - {df_rr_total.loc[date.strftime("%Y-%m-%d")].RR_feed[0]}
    рекламной аудитории - {df_rr_source.loc[date.strftime("%Y-%m-%d")].query('source == "ads"').RR_feed[0]}
    органической аудитории - {df_rr_source.loc[date.strftime("%Y-%m-%d")].query('source == "organic"').RR_feed[0]}
По мессенджеру в целом -  {df_rr_total.loc[date.strftime("%Y-%m-%d")].RR_mess[0]}
    рекламной аудитории - {df_rr_source.loc[date.strftime("%Y-%m-%d")].query('source == "ads"').RR_mess[0]}
    органической аудитории - {df_rr_source.loc[date.strftime("%Y-%m-%d")].query('source == "organic"').RR_mess[0]}

Stickiness ratio - доля DAU от MAU, %:
По новостнику в целом - {df_st_total.loc[date.strftime("%Y-%m-%d")].ST_feed[0]}
    рекламной аудитории - {df_st_source.loc[date.strftime("%Y-%m-%d")].query('source == "ads"').ST_feed[0]}
    органической аудитории - {df_st_source.loc[date.strftime("%Y-%m-%d")].query('source == "organic"').ST_feed[0]}
По мессенджеру в целом -  {df_st_total.loc[date.strftime("%Y-%m-%d")].ST_mess[0]}
    рекламной аудитории - {df_st_source.loc[date.strftime("%Y-%m-%d")].query('source == "ads"').ST_mess[0]}
    органической аудитории - {df_st_source.loc[date.strftime("%Y-%m-%d")].query('source == "organic"').ST_mess[0]}
'''
        
        # графики показателей эффективности
        sns.set()
        fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(15, 8), sharex=True)

        sns.lineplot(data=df_rr_source[['RR_feed', 'RR_mess']], estimator=min, alpha=0.1, ax=ax0)
        sns.lineplot(data=df_rr_total[['RR_feed', 'RR_mess']], legend=False, ax=ax0)
        ax0.set_title('Динамика Retention rate с границами в разрезе трафика')

        sns.lineplot(data=df_st_source[['ST_feed', 'ST_mess']], estimator=min, alpha=0.1, ax=ax1)
        sns.lineplot(data=df_st_total[['ST_feed', 'ST_mess']], legend=False, ax=ax1)
        ax1.set_title('Динамика Stickiness ratio с границами в разрезе трафика')
                
        # сохранение и отправка изображения с графиками
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Effectiveness_report.png'
        plt.close()
        
        # отправка отчетов в чат
        send_bot(chat=-867652742, message=msg, plot_object=plot_object)
           
    ## Последовательность задач
    
    # выгрузка таблиц
    df_dau = extract(query_DAU, connection)
    df_user = extract(query_user, connection)
        
    df_cont = extract(query_content, connection)
    df_post = extract(query_total_post, connection)
    
    df_action = extract(query_action, connection)
    
    df_rr_source = extract(query_rr_source, connection)
    df_rr_total = extract(query_rr_total, connection)
    df_st_source = extract(query_st_source, connection)
    df_st_total = extract(query_st_total, connection)
    
        
    # формирование и отправка отчетов с промежуточным 
    # выжиданием 5 сек. для сохранения последовательности отправки
    report_user([df_dau, df_user])
    time.sleep(5)
    report_post([df_cont, df_post]) 
    time.sleep(5)
    report_action(df_action)
    time.sleep(5)
    report_effect([df_rr_source, df_rr_total, df_st_source, df_st_total])
    
        
task_7_2_ahmadeev = task_7_2_ahmadeev()