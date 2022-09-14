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
    'figure.figsize': (20, 14)
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


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def app_report():
    
    @task()
    def extract_new_users_activity():
        query = """
            with new_users as (
              select
                distinct user_id,
                start_date
              from
                (
                  select
                    user_id,
                    min(toDate(time)) start_date
                  from
                    simulator_20220720.feed_actions
                  group by
                    user_id
                  having
                    start_date >= today() - 8
                    and start_date < today()
                  union all
                  select
                    user_id,
                    min(toDate(time)) start_date
                  from
                    simulator_20220720.message_actions
                  group by
                    user_id
                  having
                    start_date >= today() - 8
                    and start_date < today()
                ) users
            ),
            recent_users as (
              select
                distinct user_id,
                date
              from
                (
                  select
                    distinct user_id,
                    toDate(time) date
                  from
                    simulator_20220720.feed_actions
                  where
                    date >= today() - 8
                    and date < today()
                  union all
                  select
                    distinct user_id,
                    toDate(time) date
                  from
                    simulator_20220720.message_actions
                  where
                    date >= today() - 8
                    and date < today()
                ) users
            ),
            recent_new_users as (
              select
                new_users.user_id,
                start_date,
                date
              from
                new_users
                left join recent_users using user_id
            )
            select
              start_date as date,
              new_users,
              retention,
              percent_retention
            from
              (
                select
                  start_date,
                  uniq(user_id) new_users,
                  neighbor(new_users, -1, 0) previos_day_new_users,
                  countIf(user_id, start_date = date - 1) previous_day_retention,
                  neighbor(previous_day_retention, -1, 0) retention,
                  retention / previos_day_new_users percent_retention
                from
                  recent_new_users
                group by
                  start_date offset 1
              )
        """
        return ph.read_clickhouse(query=query, connection=connection)
    
    @task()
    def extract_actions():
        query = """
            with actions_by_user as (
              select
                t1.date,
                t1.user_id,
                t1.os,
                t1.gender,
                t1.country,
                t1.source,
                t1.views,
                t1.likes,
                t2.messages
              from
                (
                  select
                    toDate(time) date,
                    user_id,
                    os,
                    gender,
                    country,
                    source,
                    countIf(action = 'view') views,
                    countIf(action = 'like') likes
                  from
                    simulator_20220720.feed_actions
                  group by
                    date,
                    user_id,
                    os,
                    gender,
                    country,
                    source
                ) t1
                left join (
                  select
                    toDate(time) date,
                    user_id,
                    os,
                    gender,
                    country,
                    source,
                    count(*) messages
                  from
                    simulator_20220720.message_actions
                  group by
                    date,
                    user_id,
                    os,
                    gender,
                    country,
                    source
                ) t2 on t1.user_id = t2.user_id
                and t1.date = t2.date
            )
            select
              date,
              uniq(user_id) DAU,
              sum(views) views,
              sum(likes) likes,
              likes / views CTR,
              sum(messages) messages
            from
              actions_by_user
            group by
              date
        """
        return ph.read_clickhouse(query=query, connection=connection).iloc[-8:-1]
    
    @task()
    def extract_most_popular_posts():
        query = """
            select
              post_id,
              countIf(action = 'view') views,
              countIf(action = 'like') likes
            from
              simulator_20220720.feed_actions
            where
              toDate(time) = today() - 1
            group by
              post_id
            order by
              views desc
            limit
              50
        """
        return ph.read_clickhouse(query=query, connection=connection)
    
    @task()
    def form_message(new_users_recent_activity_df, actions_df):
        
        # DAU
        difference = actions_df.iloc[-1].DAU - actions_df.iloc[-2].DAU
        percent_difference = actions_df.iloc[-1].DAU / actions_df.iloc[-2].DAU
        dau_msg = "Активных пользователей: {}\n[на {} или на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            dau_msg = dau_msg.format(actions_df.iloc[-1].DAU, 
                                     difference, 
                                     float(f'{100*float(percent_difference-1):.2f}'),
                                     'больше')
        else:
            dau_msg = dau_msg.format(actions_df.iloc[-1].DAU, 
                                     -difference, 
                                     float(f'{100*float(1-percent_difference):.2f}'),
                                     'меньше')
        
        # новые пользователи
        difference = new_users_recent_activity_df.iloc[-1].new_users - new_users_recent_activity_df.iloc[-2].new_users
        percent_difference = new_users_recent_activity_df.iloc[-1].new_users / new_users_recent_activity_df.iloc[-2].new_users
        new_users_msg = "Новых пользователей: {}\n[на {} или на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            new_users_msg = new_users_msg.format(new_users_recent_activity_df.iloc[-1].new_users, 
                                                 difference, 
                                                 float(f'{100*float(percent_difference-1):.2f}'),
                                                 'больше')
        else:
            new_users_msg = new_users_msg.format(new_users_recent_activity_df.iloc[-1].new_users, 
                                                 -difference, 
                                                 float(f'{100*float(1-percent_difference):.2f}'),
                                                 'меньше')
            
        # retention первого дня
        difference = new_users_recent_activity_df.iloc[-1].percent_retention - new_users_recent_activity_df.iloc[-2].percent_retention
        retention_msg = "Retention первого дня: {} или {}%\n[на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            retention_msg = retention_msg.format(new_users_recent_activity_df.iloc[-1].retention, 
                                                 float(f'{100*float(new_users_recent_activity_df.iloc[-1].percent_retention):.2f}'), 
                                                 float(f'{100*float(difference):.2f}'),
                                                 'больше')
        else:
            retention_msg = retention_msg.format(new_users_recent_activity_df.iloc[-1].retention, 
                                                 float(f'{100*float(new_users_recent_activity_df.iloc[-1].percent_retention):.2f}'), 
                                                 float(f'{100*float(-difference):.2f}'),
                                                 'меньше')
        
        # просмотры в ленте
        difference = int(actions_df.iloc[-1].views) - int(actions_df.iloc[-2].views)
        percent_difference = actions_df.iloc[-1].views / actions_df.iloc[-2].views
        views_msg = "Просмотров: {}\n[на {} или на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            views_msg = views_msg.format(actions_df.iloc[-1].views, 
                                     difference, 
                                     float(f'{100*float(percent_difference-1):.2f}'),
                                     'больше')
        else:
            views_msg = views_msg.format(actions_df.iloc[-1].views, 
                                     -difference, 
                                     float(f'{100*float(1-percent_difference):.2f}'),
                                     'меньше')

        # лайки
        difference = int(actions_df.iloc[-1].likes) - int(actions_df.iloc[-2].likes)
        percent_difference = actions_df.iloc[-1].likes / actions_df.iloc[-2].likes
        likes_msg = "Лайков: {}\n[на {} или на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            likes_msg = likes_msg.format(actions_df.iloc[-1].likes, 
                                     difference, 
                                     float(f'{100*float(percent_difference-1):.2f}'),
                                     'больше')
        else:
            likes_msg = likes_msg.format(actions_df.iloc[-1].likes, 
                                     -difference, 
                                     float(f'{100*float(1-percent_difference):.2f}'),
                                     'меньше')
            
        # сообщения
        difference = int(actions_df.iloc[-1].messages) - int(actions_df.iloc[-2].messages)
        percent_difference = actions_df.iloc[-1].messages / actions_df.iloc[-2].messages
        messages_msg = "Cообщений: {}\n[на {} или на {}% {}, чем в предыдущий день]"
        if difference >= 0:
            messages_msg = messages_msg.format(actions_df.iloc[-1].messages, 
                                     difference, 
                                     float(f'{100*float(percent_difference-1):.2f}'),
                                     'больше')
        else:
            messages_msg = messages_msg.format(actions_df.iloc[-1].messages, 
                                     -difference, 
                                     float(f'{100*float(1-percent_difference):.2f}'),
                                     'меньше')
            
        msg = 'Данные за вчера'+\
              '\n\nАудитория:\n'+dau_msg+'\n'+new_users_msg+'\n'+retention_msg+\
              '\n\nСобытия:\n'+views_msg+'\n'+likes_msg+'\n'+messages_msg+\
              '\n\nСамые популярные посты:'
        
        return msg
    
    @task()
    def form_plot(new_users_recent_activity_df, actions_df):
        
        def form_lineplot(ax, x, y, title=None, xlabel=None, ylabel=None):
            plot = sns.lineplot(x, y, ax=ax)
            plot.set_title(title)
            plot.set_xlabel(xlabel)
            plot.set_xticklabels(x)
            plot.set_xticklabels(plot.get_xticklabels(), rotation=45)
            plot.set_ylabel(ylabel)

        def form_barplot(ax, x, y, title=None, xlabel=None, ylabel=None):
            plot = sns.barplot(x, y, ax=ax)
            plot.set_title(title)
            plot.set_xlabel(xlabel)
            plot.set_xticklabels(plot.get_xticklabels(), rotation=45)
            plot.set_ylabel(ylabel)
            
        fig, ax = plt.subplots(ncols=3, nrows=2)
        form_lineplot(ax[0][0], actions_df.date.dt.strftime('%d.%m'), actions_df.DAU, 'DAU')
        form_barplot(ax[0][1], new_users_recent_activity_df.date.dt.strftime('%d.%m'), new_users_recent_activity_df.new_users, 'Новые пользователи')
        form_barplot(ax[0][2], new_users_recent_activity_df.date.dt.strftime('%d.%m'), new_users_recent_activity_df.percent_retention*100, 'Retention первого дня', ylabel='%')
        form_lineplot(ax[1][0], actions_df.date.dt.strftime('%d.%m'), actions_df.views, 'Просмотры')
        form_lineplot(ax[1][1], actions_df.date.dt.strftime('%d.%m'), actions_df.likes, 'Лайки')
        form_lineplot(ax[1][2], actions_df.date.dt.strftime('%d.%m'), actions_df.messages, 'Сообщения')
        
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        
        return plot_object
    
    @task()
    def form_document(most_popular_posts_df):
        file_object = io.StringIO()
        most_popular_posts_df.to_csv(file_object, sep='|',index=False)
        file_object.name = 'most_popular_posts.csv'
        file_object.seek(0)
        return file_object
    
    @task()
    def send_report(msg, plot_object, file_object):
        bot.sendMessage(chat_id=group_chat_id, text=msg)
        bot.sendDocument(chat_id=group_chat_id, document=file_object)
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        
    new_users_recent_activity_df = extract_new_users_activity()
    actions_df = extract_actions()
    most_popular_posts_df = extract_most_popular_posts()
    msg = form_message(new_users_recent_activity_df, actions_df)
    plot_object = form_plot(new_users_recent_activity_df, actions_df)
    file_object = form_document(most_popular_posts_df)
    send_report(msg, plot_object, file_object)
    
app_report = app_report()