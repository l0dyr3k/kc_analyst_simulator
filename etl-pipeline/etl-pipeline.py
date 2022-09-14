# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20220720'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-goncharov-9',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 8, 9),
}

# Интервал запуска DAG
schedule_interval = '0 22 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_pipeline():

    @task()
    def extract_feed_actions():
        query = """
            select
              toDate(time) event_date,
              user_id,
              os,
              gender,
              age,
              countIf(action = 'view') views,
              countIf(action = 'like') likes
            from
              simulator_20220720.feed_actions
            where
              event_date = today() - 1
            group by
              event_date,
              user_id,
              os,
              gender,
              age
        """
        df = ph.read_clickhouse(query=query, connection=connection)
        return df

    @task()
    def extract_message_actions():
        query =  """
            with all_active_users as (
              select
                distinct user_id
              from
                simulator_20220720.message_actions
              where
                toDate(time) = today() - 1
              union all
              select
                distinct reciever_id as user_id
              from
                simulator_20220720.message_actions
              where
                toDate(time) = today() - 1
            ),
            active_users as (
              select
                distinct user_id
              from
                all_active_users
            ),
            feed_actions_users_info as (
              select
                distinct t1.user_id,
                os,
                gender,
                age
              from
                active_users t1
                left join simulator_20220720.feed_actions t2 using user_id
            ),
            message_users_info as (
              select
                distinct t1.user_id,
                os,
                gender,
                age
              from
                active_users t1
                left join simulator_20220720.message_actions t2 using user_id
            ),
            actions as (
              select
                active_users.user_id user_id,
                t1.messages_sent messages_sent,
                t1.users_sent users_sent,
                t2.messages_received messages_received,
                t2.users_received users_received
              from
                active_users
                left join (
                  select
                    user_id,
                    count(reciever_id) messages_sent,
                    count(distinct reciever_id) users_sent
                  from
                    simulator_20220720.message_actions
                  where
                    toDate(time) = today() - 1
                  group by
                    user_id
                ) t1 on active_users.user_id = t1.user_id
                left join (
                  select
                    reciever_id,
                    count(user_id) messages_received,
                    count(distinct user_id) users_received
                  from
                    simulator_20220720.message_actions
                  where
                    toDate(time) = today() - 1
                  group by
                    reciever_id
                ) t2 on active_users.user_id = t2.reciever_id
            )
            select
              today() - 1 event_date,
              actions.user_id user_id,
              coalesce(
                feed_actions_users_info.os,
                message_users_info.os
              ) os,
              coalesce(
                feed_actions_users_info.gender,
                message_users_info.gender
              ) gender,
              coalesce(
                feed_actions_users_info.age,
                message_users_info.age
              ) age,
              messages_sent,
              users_sent,
              messages_received,
              users_received
            from
              actions
              left join feed_actions_users_info on actions.user_id = feed_actions_users_info.user_id
              left join message_users_info on actions.user_id = message_users_info.user_id
        """
        df = ph.read_clickhouse(query=query, connection=connection)
        return df
    
    @task()
    def join_feed_actions_and_message_actions(feed_actions, message_actions):
        df = feed_actions.merge(message_actions, on='user_id', how='outer', copy=False, suffixes=('', '_y'))
        df['event_date'] = df['event_date'].fillna(df['event_date_y'])
        df['os'] = df['os'].fillna(df['os_y'])
        df['gender'] = df['gender'].fillna(df['gender_y'])
        df['age'] = df['age'].fillna(df['age_y'])
        df = df[['event_date', 'user_id', 'os', 'gender', 'age', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df = df.fillna(0)
        return df
    
    @task()
    def transfrom_os(df):
        df_os = df[['event_date', 'os', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['event_date', 'os'], as_index=False)\
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'})
        df_os['dimension'] = 'os'
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        return df_os
    
    @task()
    def transfrom_gender(df):
        df_gender = df[['event_date', 'gender', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['event_date', 'gender'], as_index=False)\
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'})
        df_gender['dimension'] = 'gender'
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        return df_gender
    
    @task()
    def transfrom_age(df):
        df_age = df[['event_date', 'age', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['event_date', 'age'], as_index=False)\
            .agg({'likes': 'sum', 'views': 'sum', 'messages_received': 'sum', 'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'})
        df_age['dimension'] = 'age'
        df_age = df_age.rename(columns={'age': 'dimension_value'})
        return df_age
    
    @task()
    def load(df_os, df_gender, df_age):
        connection_test = {
            'host':'https://clickhouse.lab.karpov.courses',
            'user':'student-rw',
            'password': '656e2b0c9c',
            'database': 'test'
        }
        df_test = pd.concat([df_os, df_gender, df_age])
        df_test = df_test[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_test = df_test.astype({'views': int, 'likes': int, 'messages_received': int, 'messages_sent': int, 'users_received': int, 'users_sent': int})
        query = """
            create table if not exists test.e_goncharov_9 (
              event_date Date,
              dimension String,
              dimension_value String,
              views Int64,
              likes Int64,
              messages_received Int64,
              messages_sent Int64,
              users_received Int64,
              users_sent Int64
            ) engine = Log()
        """
        ph.execute(query=query, connection=connection_test)
        ph.to_clickhouse(df=df_test, table='e_goncharov_9', index=False, connection=connection_test)
        
    feed_actions = extract_feed_actions()
    message_actions = extract_message_actions()
    actions = join_feed_actions_and_message_actions(feed_actions, message_actions)
    df_os = transfrom_os(actions)
    df_gender = transfrom_gender(actions)
    df_age = transfrom_age(actions)
    load(df_os, df_gender, df_age)
    
    
etl_pipeline = etl_pipeline()