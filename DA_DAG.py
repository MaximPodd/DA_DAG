import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse 
from typing import Optional
import matplotlib.dates as mdates
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm.poddub',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 2),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

my_token = '7229672972:AAEmccpV3JkBSoAuClS7baPzxNZ1k9EBgyY' 
chat_id = -969316925 #-1002614297220    1445087139
bot = telegram.Bot(token=my_token)
yesterday = (datetime.now() - timedelta(days=1)).date()


metrics_params = {
    'views':{'a':3,'n':7,'url':'http://superset.lab.karpov.courses/r/6546', 'name':'Количество просмотров'},
    'likes':{'a':3,'n':5,'url':'http://superset.lab.karpov.courses/r/6543', 'name':'Количество лайков'},
    'CTR':{'a':4,'n':6,'url':'http://superset.lab.karpov.courses/r/6545', 'name':'CTR'},
    'users_lenta':{'a':3,'n':8,'url':'http://superset.lab.karpov.courses/r/6542', 'name':'Уникальные пользователи лента'},
    'messages':{'a':2.5,'n':8,'url':'http://superset.lab.karpov.courses/r/6547', 'name':'Количество сообщений'},
    'users_message':{'a':2.5,'n':10,'url':'http://superset.lab.karpov.courses/r/6541', 'name':'Уникальные пользователи мессенджер'}
}


query = ''' 
                    SELECT 
                        ts,
                        date,
                        hm,
                        users_message,
                        messages,
                        users_lenta,
                        views,
                        likes,
                        likes / views AS CTR
                    FROM 
                    (
                        SELECT
                            toStartOfFifteenMinutes(time) AS ts, 
                            toDate(ts) AS date,
                            formatDateTime(ts, '%R') AS hm, 
                            uniqExact(user_id) AS users_message,
                            COUNT(user_id) AS messages
                        FROM simulator_20250520.message_actions
                        WHERE ts >= today() - 14 AND ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                    ) AS messages
                    FULL OUTER JOIN
                    (
                        SELECT
                            toStartOfFifteenMinutes(time) AS ts, 
                            toDate(ts) AS date,
                            formatDateTime(ts, '%R') AS hm, 
                            uniqExact(user_id) AS users_lenta,
                            countIf(user_id, action = 'view') AS views,
                            countIf(user_id, action = 'like') AS likes
                        FROM simulator_20250520.feed_actions
                        WHERE ts >= today() - 14 AND ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                    ) AS feed
                    USING (ts, date, hm)
                    ORDER BY ts '''


def check_anomaly(df, metric, a=3, n=5):
    
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n,center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n,center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task_9_podd():
    
    @task()
    def extract_data(query):
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20250520',
                      'user':'student',
                      'password':'dpo_python_2020'
                     }

        df = pandahouse.read_clickhouse(query, connection=connection)

        return df

    @task()
    def run_alerts(data, chat=None):
        chat_id = chat or 1445087139
        bot = telegram.Bot(token=my_token)

        data = data.copy()

        # Фильтрация данных за последние 48 часов
        last_ts = data['ts'].max()
        cutoff_48h = last_ts - pd.Timedelta(hours=48)
        data_48h = data[data['ts'] >= cutoff_48h].copy()

        metric_list = ['users_lenta','views','likes','CTR','messages','users_message']
        for metric in metric_list:
            df = data_48h[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric,a=metrics_params[metric]['a'], n=metrics_params[metric]['n'])  

            if is_alert == 1:
                msg = """
                    <b>🚨</b> {name}
                    <b>Текущее значение</b> {current_value:.2f}
                    <b>Отклонение</b> {last_val_diff:.0%}

                    <a href="{url}">🔗 Чарт метрики</a>
                    <a href="http://superset.lab.karpov.courses/r/6540">📊 Дашборд</a>

                    🦸‍♂️@UnderWood
                """

                msg = "\n".join(line.strip() for line in msg.strip().splitlines()).format(
                    name=metrics_params[metric]['name'],
                    url=metrics_params[metric]['url'],
                    current_value=df[metric].iloc[-1],
                    last_val_diff=abs(1 - (df[metric].iloc[-1] / df[metric].iloc[-2]))
                )

                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()

                fig, ax = plt.subplots(figsize=(16, 10))

                # Рисуем линии метрики и границ
                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='upper bound')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='lower bound')

                # Выделяем аномальные точки (те, что выходят за границы)
                anomalies_mask = (df[metric] > df['up']) | (df[metric] < df['low'])
                anomalies = df[anomalies_mask]
                if not anomalies.empty:
                    ax.scatter(anomalies['ts'], anomalies[metric], color='red', label='anomaly', s=100)

                # Особо выделяем последнюю точку, если она аномальная
                if is_alert:
                    last_point = df.iloc[-1]
                    ax.scatter(last_point['ts'], last_point[metric], color='darkred', label='current alert', s=150, marker='X')

                # Форматирование оси времени
                ax.xaxis.set_major_locator(mdates.AutoDateLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
                plt.xticks(rotation=45)

                ax.set(xlabel='Time (last 48 hours)')
                ax.set(ylabel=metric)
                ax.set_title('{} (last 48 hours)'.format(metric))
                ax.set(ylim=(0, None))
                ax.legend()

                # Формируем файловый объект
                plot_object = io.BytesIO()
                fig.savefig(plot_object, bbox_inches='tight', dpi=300)
                plot_object.seek(0)
                plot_object.name = '{0}_48h.png'.format(metric)
                plt.close(fig)

                # Отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        # bot.sendMessage(chat_id=chat_id, text='sheck')

        
    df = extract_data(query)
    run_alerts(df,chat_id)

    
    
task_9_podd = task_9_podd()