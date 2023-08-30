import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Функция загрузки данных из CH
def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230620',
              'user':'student',
              'password':'dpo_python_2020'
             }


query_first_part = '''
                        SELECT 
                            count(distinct user_id) DAU,
                            countIf(action='view') views,
                            countIf(action='like') likes,
                            round(100 * likes / views, 2) CTR
                        FROM simulator_20230620.feed_actions 
                        where toDate(time) = yesterday()
                   '''

query_second_part = '''
                        SELECT
                            toDate(time) date,
                            count(distinct user_id) DAU,
                            countIf(action='view') views,
                            countIf(action='like') likes,
                            round(100 * likes / views, 2) CTR
                        FROM simulator_20230620.feed_actions 
                        where toDate(time) between 
                                            toDate((today() - 7))
                                            and
                                            toDate((today() - 1))
                        group by date
                    '''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
                'owner': 'm.ahmadeev',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 7, 8)
               }

# Интервал запуска DAG - за 10 минут до требуемого времени отправки (запас под время исполнения)
schedule_interval = '50 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task_7_1_ahmadeev():
    
    # функция выгрузки
    @task()
    def extract(query, connection):
        df = ch_get_df(query, connection)
        return df
    
    # функция отправки 1 части отчета
    @task()
    def report_1(df, chat=None):
        
        # получение доступа к боту и id чата
        chat_id = chat or 377590876
        my_token = '632525***vko'
        bot = telegram.Bot(token=my_token)
        
        # формирование и отправка 1 части отчета в виде сообщения
        date = datetime.date(datetime.today() - timedelta(days=1))
        msg = f'''Ключевые метрики за {date.strftime("%d.%m.%Y")}:\nDAU - {df['DAU'][0]}\nПросмотры - {df['views'][0]}\nЛайки - {df['likes'][0]}\nCTR - {df['CTR'][0]} %'''
        bot.sendMessage(chat_id=chat_id, text=msg)
        
    # функция отправки 2 части отчета
    @task()
    def report_2(df, chat=None):
        
        # получение доступа к боту и id чата
        chat_id = chat or 377590876
        my_token = '632525***vko'
        bot = telegram.Bot(token=my_token)
        
        # преобразование к формату даты и установление индекса
        df['date'] = pd.to_datetime(df.date)
        df.set_index('date', inplace=True)
        
        # отрисовка графиков для второй части отчета
        sns.set()
        fig, axes = plt.subplots(4, 1, figsize=(15, 16), sharex=True)
        for ax, col in zip(axes.flatten(), df.columns):
            sns.lineplot(data=df[col], ax=ax)
            ax.set_title(col)
        date_1 = datetime.date(datetime.today() - timedelta(days=7))
        date_2 = datetime.date(datetime.today() - timedelta(days=1))
        fig.suptitle(f'Отчет за период {date_1.strftime("%d.%m.%Y")} - {date_2.strftime("%d.%m.%Y")}', fontsize=40)
        
        # сохранение и отправка изображения с графиками
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Report_plots.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    ## Последовательность задач
    
    # выгрузка таблиц
    df_rep1 = extract(query_first_part, connection)
    df_rep2 = extract(query_second_part, connection)
        
    # формирование и отправка отчетов
    report_1(df_rep1, chat=-867652742)
    report_2(df_rep2, chat=-867652742)
        
    
task_7_1_ahmadeev = task_7_1_ahmadeev()