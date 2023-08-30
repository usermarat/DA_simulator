from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


## Функции 
# загрузки данных из CH
def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

# загрузки таблицы в CH
def ch_load_df(df, table_name, query_create, connection):
    ph.execute(query_create, connection=connection)
    ph.to_clickhouse(df, table_name, connection=connection, index=False)
    
## Соединения
# С основной БД
connection_sim = {'host': 'https://clickhouse.lab.karpov.courses',
                  'user':'student',
                  'password':'dpo_python_2020'
                 }

# С БД test
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database': 'test',
                   'user': 'student-rw',
                   'password': '656e2b0c9c'
                  }

# Расчетные метрики
metrics = ['views', 'likes', 'messages_received',
           'messages_sent','users_received',
           'users_sent']

## Запросы
# В feed_actions для каждого юзера посчитаем число просмотров и лайков контента за вчера
query_feed = ''' 
                SELECT 
                    user_id as user,  
                    countIf(action='view') views,
                    countIf(action='like') likes,
                    min(age) age,
                    min(gender) gender,
                    min(os) os
                FROM simulator_20230620.feed_actions 
                where toDate(time) = yesterday()
                group by user_id
             '''
# В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений,
# скольким людям он пишет, сколько людей пишут ему
query_message = '''
                    SELECT * FROM
                    (
                        SELECT * FROM
                            (
                                SELECT
                                    user_id as user,
                                    count(*) messages_sent,
                                    count(distinct reciever_id) users_sent  
                                FROM simulator_20230620.message_actions
                                where toDate(time) = yesterday()
                                group by user_id
                            ) t1
                        full outer join 
                            (
                                SELECT 
                                    reciever_id as user,
                                    count(*) messages_received,
                                    count(distinct user_id) users_received
                                FROM simulator_20230620.message_actions 
                                where toDate(time) = yesterday()
                                group by reciever_id
                            ) t2
                        using user
                    ) t1t2
                    left join
                    (
                        SELECT distinct user_id as user,
                                        age,
                                        gender,
                                        os
                        FROM simulator_20230620.feed_actions 
                        
                        union distinct 
                        
                        SELECT distinct user_id as user,
                                        age,
                                        gender,
                                        os
                        FROM simulator_20230620.message_actions 
                    ) t3
                    using user
                '''
# Создание таблицы в схеме test
query_test = '''
                CREATE table if not exists 
                       test.task6_ahmadeev
                           (
                            event_date Date,
                            dimension String,
                            dimension_value String,
                            views Int64,
                            likes Int64,
                            messages_received Int64,
                            messages_sent Int64,
                            users_received Int64,
                            users_sent Int64
                           )
                ENGINE = MergeTree()
                order by event_date
              '''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
                'owner': 'm.ahmadeev',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 7, 6)
               }

# Интервал запуска DAG
schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task_6_ahmadeev():
    
    # функция выгрузки
    @task()
    def extract(query, connection):
        df = ch_get_df(query, connection)
        return df
    
    # функция объединения
    @task()
    def merge_df(df1, df2):
        df = pd.merge(df1,
                      df2, 
                      how='outer', 
                      on=['user', 'age',
                          'gender', 'os']
                     )
        df.fillna(0, inplace=True)
        return df
    
    # функция расчета агрегаций
    @task
    def transfrom(df, dimension):
        df_transf = df.groupby(dimension, as_index=False)[metrics].sum()
        df_transf[metrics] = df_transf[metrics].astype(int)
        df_transf['dimension'] = dimension
        df_transf.rename(columns={dimension: 'dimension_value'}, inplace=True)
        return df_transf
    
    # функция загрузки
    @task
    def load(list_df, table_name, query, connection):
        context = get_current_context()
        ds = context['ds']
        
        df_final = pd.concat(list_df)
        df_final['event_date'] = datetime.date(
                                               datetime.today() - timedelta(days=1)
                                              ).strftime('%Y-%m-%d')
        df_final = df_final[['event_date', 'dimension', 'dimension_value'] + metrics]
        ch_load_df(df_final, table_name, query, connection)
        
        print(f'Data for {ds} loaded')
    
    ## Последовательность задач
    
    # выгрузка таблиц
    df_feed = extract(query_feed, connection_sim)
    df_message = extract(query_message, connection_sim)
    
    # объединение в одну
    df_merged = merge_df(df_feed, df_message)
    
    # расчет агрегированных метрик в разрезе трех характеристик
    df_gender = transfrom(df_merged, 'gender')
    df_os = transfrom(df_merged, 'os')
    df_age = transfrom(df_merged, 'age')
    
    # загрузка в создаваемую/созданную таблицу
    load([df_os, df_gender, df_age], 'task6_ahmadeev', query_test, connection_test)
        
    
task_6_ahmadeev = task_6_ahmadeev()