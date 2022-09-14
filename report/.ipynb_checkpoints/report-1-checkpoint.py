from datetime import datetime, timedelta
import pandahouse as ph
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram


rc = {
    'figure.figsize': (16, 12)
}
sns.set(rc=rc, style='whitegrid')

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
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 8),
}

schedule_interval = '0 11 * * *'

bot = telegram.Bot(token='5453075474:AAHXD2BzvRFjQkwNFow4xEHC-yAeIJ_raG0')

group_chat_id = -770113521


def percent(number):
    return float(f'{100*number:.2f}')


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report():
    
    @task()
    def extract_data():
        query = """
            select
              toDate(time) date,
              uniq(user_id) DAU,
              countIf(action = 'view') views,
              countIf(action = 'like') likes,
              likes / views CTR
            from
              simulator_20220720.feed_actions
            where
              date > today() - 9 and date < today()
            group by
              date
        """
        df = ph.read_clickhouse(query=query, connection=connection)
        return df
    
    @task()
    def form_message(df):
        msg = f"""
        Статистика за вчера
        DAU: {df.iloc[-1].DAU} пользователей
        Просмотров: {df.iloc[-1].views}
        Лайков: {df.iloc[-1].likes}
        CTR: {percent(df.iloc[-1].CTR)}%
        """
        return msg
    
    @task()
    def form_plots(df):
        
        def form_lineplot(ax, x, y, title='title', xlabel='xlabel', ylabel='ylabel'):
            plot = sns.lineplot(x, y, ax=ax)
            plot.set_title(title)
            plot.set_xlabel(xlabel)
            plot.set_ylabel(ylabel)
            
        fig, axs = plt.subplots(ncols=2, nrows=2)
        form_lineplot(axs[0][0], x=df.date.dt.strftime('%d.%m'), y=df.DAU, title='DAU', xlabel='Day', ylabel='Users')
        form_lineplot(axs[0][1], x=df.date.dt.strftime('%d.%m'), y=df.CTR.map(lambda x: percent(x)), title='CTR', xlabel='Day', ylabel='Percent')
        form_lineplot(axs[1][0], x=df.date.dt.strftime('%d.%m'), y=df.views, title='Views', xlabel='Day', ylabel=None)
        form_lineplot(axs[1][1], x=df.date.dt.strftime('%d.%m'), y=df.likes, title='Likes', xlabel='Day', ylabel=None)
        plt.suptitle('Last week data')
        
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        
        return plot_object
    
    @task()
    def send_report(chat_id, msg, plot_object):
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    df_data = extract_data()
    msg = form_message(df_data)
    plot_object = form_plots(df_data)
    send_report(group_chat_id, msg, plot_object)
    
feed_report = feed_report()