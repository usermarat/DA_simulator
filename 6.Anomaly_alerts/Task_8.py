import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from itertools import product

# функция проверки аномалий
def check_anomaly(df, mtr, a=3, up=1, low=1):
    
    """
    Используется комбинация двух статистических методов:
    - правило сигм и
    - межквартильный размах
    на трех способах расчета скользящих значений:
    - аналогичные 15-минутки последних 7 дней (что позволяет учесть небольшие различия по дням недели - 
    по некоторым метрикам присутствует внутринедельная сезонность, выраженная в меньшей активности по 
    некоторым дням)
    - аналогичные 15-минутки прошлых суток (5 значений прошлых суток - берется аналогичная 15-минутка
    прошлых суток и по две до и после нее, что позволяет сгладить колебания метрики внутри часа, особенно
    в точках ее экстремумов)
    - предшествующие 15-минутки (5 предшествующих значений, что позволяет учесть динамику метрики
    текущего дня)
    
    Все рассчитанные по данной методике границы (всего получается по 6 значений - 2 метода Х 3 способа)
    учитываются для расчета итоговых верхней и нижней границ с разной степенью влияния: 
    К полученным значениям верхних и нижних границ применяются веса по формуле экспоненты, занижающей влияние 
    на итоговый расчет границы экстремально высоких (для верхней границы) и экстремально низких (для нижней) 
    значений, рассчитанных по указанным методикам границ. 
    
    Данный подход позволяет "штрафовать" модель за слишком широкие диапазоны границ.
    Получается своего рода ансамбль методов с голосами, имеющими разные веса.
    Дополнительно в формуле добавлены гиперпараметры, позволяющие регулировать степень регуляризации отдельно
    для верхней и нижней границы - значения ниже единицы раздвигают итоговые границы (повышают верхнюю и понижают
    нижнюю границу), значения выше единицы сужают ее (соответственно, понижают верхнюю и повышают нижнюю границу),
    при этом возможны комбинации разных значений данных гиперпараметров для разно- и однонаправленных изменений границ.
    Это позволяет более гибко подстраивать модель под текущую динамику метрик.
    
    Parameters
    ----------
        df : pandas.core.frame.DataFrame
            DataFrame to check for anomalies (must contain 15-minutes chunks for the last 8 days)
        mtr : str
            Metric on which boundaries are calculated
        a : int, optional
            Hyperparameter influence the boundaries' width deviation (the bigger the wider, default is 3)
        up : positive int/float, optional
            Hyperparameter influence upper bound regularization (values more than 1 gives more weight for
            the votes of the lower bounds causing the final upper bound move lower, values less than 1 
            on the countrary gives more weight to the higher bounds causing the final upper bound move higher,
            default is 1)
        low : positive int/float, optional
            Hyperparameter influence lower bound regularization (reversed to the up hyperparameter:
            values more than 1 gives more weight for the votes of the higher bounds causing the final lower bound
            move higher, values less than 1 on the countrary gives more weight to the lower bounds causing
            the final lower bound move lower, default is 1)
    
    Returns
    -------
        flag : boolean
            trigger for metric's boundary break for the last timestamp (15-minutes chunks)
        df_last : pandas.core.frame.DataFrame
            DataFrame with calculated final boundaries
    """
    
    ## для начала считаем 15-минутки последних 7 дней
    # межквартильный размах
    
    # для расчета берем данные за предшествующие 7 дней в датафрейме,
    # то есть все первые строки датафрейма до (24 х 60) / 15 = 96 последних строк
    df_quant = df.iloc[:-96].groupby('hm').quantile(0.25)
    df_quant.rename(lambda x: 'q25', axis=1, inplace=True)
    df_last = df.iloc[-96:].join(df_quant, on='hm')
    df_quant = df.iloc[:-96].groupby('hm').quantile(0.75)
    df_quant.rename(lambda x: 'q75', axis=1, inplace=True)
    df_last = df_last.join(df_quant, on='hm')
    df_last['iqr'] = df_last['q75'] - df_last['q25']
    df_last['upper'] = df_last['q75'] + a * df_last['iqr']
    df_last['lower'] = df_last['q25'] - a * df_last['iqr']
    
    # сглаживаем значения полученных границ, используя 7 значений в окне (равно количеству значений в расчете)
    df_last['upper_week_quant'] = df_last.upper.rolling(7, center=True, min_periods=1).mean()
    df_last['lower_week_quant'] = df_last.lower.rolling(7, center=True, min_periods=1).mean()
    
    # правило сигм
    df_std = df.iloc[:-96].groupby('hm').std(ddof=1)
    df_std.rename(lambda x: 'std', axis=1, inplace=True)
    df_last = df_last.join(df_std, on='hm')
    df_mean = df.iloc[:-96].groupby('hm').mean()
    df_mean.rename(lambda x: 'mean', axis=1, inplace=True)
    df_last = df_last.join(df_mean, on='hm')
    df_last['upper'] = df_last['mean'] + a * df_last['std']
    df_last['lower'] = df_last['mean'] - a * df_last['std']
    
    df_last['upper_week_std'] = df_last.upper.rolling(7, center=True, min_periods=1).mean()
    df_last['lower_week_std'] = df_last.lower.rolling(7, center=True, min_periods=1).mean()
    
    ## 15-минутки прошлых суток
    # межквартильный размах
    df_last['q25'] = df[mtr].shift(periods=1, freq="D").rolling(5, center=True).quantile(0.25)
    df_last['q75'] = df[mtr].shift(periods=1, freq="D").rolling(5, center=True).quantile(0.75)
    df_last['iqr'] = df_last['q75'] - df_last['q25']
    df_last['upper'] = df_last['q75'] + a * df_last['iqr']
    df_last['lower'] = df_last['q25'] - a * df_last['iqr']
    
    # здесь уже сглаживание на 5 значениях в окне, т.к. считали показатели по 5 прошлым значениям
    df_last['upper_day_quant'] = df_last.upper.rolling(5, center=True, min_periods=1).mean()
    df_last['lower_day_quant'] = df_last.lower.rolling(5, center=True, min_periods=1).mean()
    
    # правило сигм
    df_last['std'] = df[mtr].shift(periods=1, freq="D").rolling(5, center=True).std()
    df_last['mean'] = df[mtr].shift(periods=1, freq="D").rolling(5, center=True).mean()
    df_last['upper'] = df_last['mean'] + a * df_last['std']
    df_last['lower'] = df_last['mean'] - a * df_last['std']
    
    df_last['upper_day_std'] = df_last.upper.rolling(5, center=True, min_periods=1).mean()
    df_last['lower_day_std'] = df_last.lower.rolling(5, center=True, min_periods=1).mean()
    
    ## предшествующие 15-минутки
    # межквартильный размах
    # из расчета исключаем текущую 15-минутку
    df_last['q25'] = df[mtr].rolling(5, center=False, closed='left').quantile(0.25)
    df_last['q75'] = df[mtr].rolling(5, center=False, closed='left').quantile(0.75)
    df_last['iqr'] = df_last['q75'] - df_last['q25']
    df_last['upper'] = df_last['q75'] + a * df_last['iqr']
    df_last['lower'] = df_last['q25'] - a * df_last['iqr']
    
    df_last['upper_hour_quant'] = df_last.upper.rolling(5, center=True, min_periods=1).mean()
    df_last['lower_hour_quant'] = df_last.lower.rolling(5, center=True, min_periods=1).mean()
    
    # правило сигм
    df_last['std'] = df[mtr].rolling(5, center=False, closed='left').std()
    df_last['mean'] = df[mtr].rolling(5, center=False, closed='left').mean()
    df_last['upper'] = df_last['mean'] + a * df_last['std']
    df_last['lower'] = df_last['mean'] - a * df_last['std']
    
    df_last['upper_hour_std'] = df_last.upper.rolling(5, center=True, min_periods=1).mean()
    df_last['lower_hour_std'] = df_last.lower.rolling(5, center=True, min_periods=1).mean()
    
    # функции для расчета весов для верхней и нижней границ
    def upper_bound(row):
        return (np.array(row) @ (1/np.exp(up*row/np.max(row)))) / np.sum(1/np.exp(up*row/max(row)))
    
    def lower_bound(row):
        return (np.array(row) @ np.exp(low*row/np.min(row))) / np.sum(np.exp(low*row/min(row)))
    
    # расчет итоговых границ с весами
    df_last['upper'] = df_last[['upper_week_quant', 
                                'upper_week_std',
                                'upper_day_quant',
                                'upper_day_std',
                                'upper_hour_quant',
                                'upper_hour_std'
                               ]].apply(upper_bound, axis=1)
    
    df_last['lower'] = df_last[['lower_week_quant', 
                                'lower_week_std',
                                'lower_day_quant',
                                'lower_day_std',
                                'lower_hour_quant',
                                'lower_hour_std'
                               ]].apply(lower_bound, axis=1)
    
    # проверка на нарушение границ метрики последней 15-минутки
    if df_last[mtr].iloc[-1] < df_last['lower'].iloc[-1] or df_last[mtr].iloc[-1] > df_last['upper'].iloc[-1]:
        flag = True
    else:
        flag = False
    
    return flag, df_last[[mtr, 'upper', 'lower']]


# Дефолтные параметры, которые прокидываются в таски
default_args = {
                'owner': 'm.ahmadeev',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 7, 15)
               }

# Интервал запуска DAG - каждые 15 минут с рассинхроном с выгружаемыми 15-минутками,
# во избежание возможных потерь периодов на стыках временных интервалов
schedule_interval = '2,17,32,47 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task_8_ahmadeev():
    
    @task()
    def run_alerts(chat=None):
        # доступ к боту и чату
        chat_id = chat or 377590876
        my_token = '632525***vko'
        bot = telegram.Bot(token=my_token)

        # выгрузка данных из Кликхаус
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230620',
                      'user':'student',
                      'password':'dpo_python_2020'
                     }

        query = ''' SELECT
                        ts,
                        hm,
                        os,
                        dau_feed,
                        views,
                        likes,
                        ctr,
                        dau_mess,
                        messages
                    FROM
                    (
                        SELECT
                            toStartOfFifteenMinutes(time) as ts,
                            formatDateTime(ts, '%R') as hm,
                            os,
                            count(distinct user_id) dau_feed,
                            countIf(action='view') views,
                            countIf(action='like') likes,
                            round(100 * likes / views, 2) ctr
                        FROM simulator_20230620.feed_actions
                        WHERE 
                            ts >=  addDays(toStartOfFifteenMinutes(now()), -8) and 
                            ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, hm, os
                    ) t1
                    join
                    (
                        SELECT
                            toStartOfFifteenMinutes(time) as ts,
                            formatDateTime(ts, '%R') as hm,
                            os, 
                            count(distinct user_id) dau_mess,
                            count(user_id) messages
                        FROM simulator_20230620.message_actions
                        WHERE 
                            ts >=  addDays(toStartOfFifteenMinutes(now()), -8) and 
                            ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, hm, os
                    ) t2
                    using ts, hm, os
                    ORDER BY ts 
                '''
        df = ph.read_clickhouse(query, connection=connection)
        df.set_index('ts', inplace=True)

        # рассчитываем каждую метрику в разрезе операционных систем
        slices = ['iOS', 'Android']
        metrics = ['dau_feed', 'views', 'likes', 'ctr', 'dau_mess', 'messages']

        for mtr, slc in product(metrics, slices):
            
            # по итогам наблюдения отработки модели были подобраны наиболее оптимальные
            # гиперпараметры регуляризации, в том числе исходя из следующих предпосылок:
            # - резкое падение метрики является более критичным для детекции в отличие от резкого роста,
            # т.к. это может свидетельствовать о критических поломках в работе приложения, соответственно,
            # лучше нижнюю допустимую границу поднять как можно выше
            # - с учетом новизны приложения и более "взрывного" расширения нашей аудитории для снижения 
            # в связи с этим числа ложноположительных отработок по верхней границе ее допустимый уровень
            # так же следует поднять
            
            anomaly, df_checked = check_anomaly(df.query('os == @slc')[['hm', mtr]], mtr, up=0.6, low=1.5)

            if anomaly:
                diff = min(abs(1 - df_checked[mtr].iloc[-1] / df_checked['lower'].iloc[-1]),
                           abs(1 - df_checked[mtr].iloc[-1] / df_checked['upper'].iloc[-1]))
                msg = f'''Метрика {mtr} в срезе {slc}\nТекущее значение {df_checked[mtr].iloc[-1]}\nОтклонение от допустимой границы на {diff:.2%}\nСсылка на дашборд http://superset.lab.karpov.courses/r/4059
                       '''

                sns.set(rc={'figure.figsize':(15, 8)})                
                sns.lineplot(data=df_checked[[mtr, 'upper', 'lower']])
                plt.title(f'{mtr} in {slc}')

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{mtr} in {slc}.png'
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    run_alerts(chat=-991591198)
    
task_8_ahmadeev = task_8_ahmadeev()