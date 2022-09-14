import pandas as pd
import pandahouse as ph
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram


rc = {
    'figure.figsize': (14, 10)
}
sns.set(rc=rc, style='whitegrid')
plt.tight_layout()

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20220720'
}

default_args = {
    'owner': 'e-goncharov-9',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 8, 8),
}

schedule_interval = '*/15 * * * *'

bot = telegram.Bot(token='5453075474:AAHXD2BzvRFjQkwNFow4xEHC-yAeIJ_raG0')

chat_id = -788021703


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kc_alert():
    
    @task()
    def extract_data():
        query = """
            select
              *
            from
              (
                select
                  toStartOfFifteenMinutes(time) period,
                  uniq(user_id) users_feed,
                  countIf(action = 'view') views,
                  countIf(action = 'like') likes,
                  round(likes / views, 3) CTR
                from
                  simulator_20220720.feed_actions
                where
                  time >= date_add(day, -1, toStartOfFifteenMinutes(now()))
                  and time < toStartOfFifteenMinutes(now())
                group by
                  period
              ) feed
              join (
                select
                  toStartOfFifteenMinutes(time) period,
                  uniq(user_id) users_messages,
                  count(*) messages
                from
                  simulator_20220720.message_actions
                where
                  time >= date_add(day, -1, toStartOfFifteenMinutes(now()))
                  and time < toStartOfFifteenMinutes(now())
                group by
                  period
              ) messages using period
              order by period
        """
        return ph.read_clickhouse(query=query, connection=connection)

    def check_anomaly_sigma(df, metric, n, a):
        # функция выявления аномалий по правилу сигм
        df['rolling_mean'] = df[metric].shift(1).rolling(n).mean()
        df['rolling_std'] = df[metric].shift(1).rolling(n).std()
        df['low'] = df['rolling_mean'] - a*df['rolling_std']
        df['up'] = df['rolling_mean'] + a*df['rolling_std']
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        if df.iloc[-1][metric] < df.iloc[-1].low or df.iloc[-1][metric] > df.iloc[-1].up:
            return True
        else:
            return False
        
    def check_anomaly_iqr(df, metric, n, a):
        # функция выявления аномалий по доверительному интеравлу, основанному на межквартильном размахе
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['low'] = df['q25'] - a*df['iqr']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        if df.iloc[-1][metric] < df.iloc[-1].low or df.iloc[-1][metric] > df.iloc[-1].up:
            return True
        else:
            return False
        
    def send_alert_message(df, metric):
        # функция, которая отправляет сообщение с алертом в телеграм
        diff = df[metric].iloc[-1] / df[metric].iloc[-2]
        if diff > 1:
            diff = diff - 1
        else:
            diff = 1 - diff
        msg = 'Время: {}\nМетрика: {}\nЗначение: {}\nОтклонение от предыдущего показателя: {}%'.format(df.period.dt.strftime('%d.%m.%Y | %H:%M').iloc[-1], 
                                                                                                       metric, 
                                                                                                       df[metric].iloc[-1], 
                                                                                                       round(diff*100, 2))
        bot.sendMessage(chat_id=chat_id, text=msg)

        ax = sns.lineplot(data=df, x='period', y=metric, label=metric)
        ax = sns.lineplot(data=df, x='period', y='low', label='low')
        ax = sns.lineplot(data=df, x='period', y='up', label='up')
        for ind, label in enumerate(ax.get_xticklabels()):
            label.set_visible(True)
            label.set_rotation(30)
        ax.set_xlabel(None)
        ax.set_ylabel(None)
        ax.set_title(metric)
        ax.set(ylim=(0, None))

        plot_object = io.BytesIO()
        ax.figure.savefig(plot_object)
        plot_object.seek(0)
        plt.clf()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task()
    def check_alerts(df):
        # функция проведения тестов на выявление аномалий
        # чтобы наблюдение было принято за анамалию, оно должно завалить все тесты 
        metrics = ['users_feed', 'views', 'likes', 'CTR', 'users_messages', 'messages']
        for metric in metrics:
            is_anomaly = []
            is_anomaly.append(check_anomaly_sigma(df.copy(), metric, 5, 3))
            is_anomaly.append(check_anomaly_iqr(df, metric, 7, 3))
            if all(is_anomaly):
                send_alert_message(df, metric)
                
    df = extract_data()
    check_alerts(df)
    
kc_alert = kc_alert()