# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse
from io import StringIO
import requests
from datetime import date, datetime, timedelta


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# подключение к базе для считывания
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                    'database':'simulator_20230820',
                    'user':'student',
                    'password':'dpo_python_2020'}

# подключение к базе для записи
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                     'database':'test',
                     'user':'student-rw', 
                     'password':'656e2b0c9c'}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-biletskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 26),
}


# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def arseniy_dag1_task8():
    
    #Создаем DateFrame для записи данных из таблицы feed_actions
    @task
    def extract_feed():
        q_feed = """
        SELECT user_id as user_id,
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            toDate(time) as event_date,
            os,
            gender,
            age

        FROM {db}.feed_actions 
        WHERE toDate(time) = today() - 1

        GROUP BY toDate(time),user_id,os,gender,age
        """
        df1 = pandahouse.read_clickhouse(q_feed, connection=connection)
        return df1
        
    #Создаем DateFrame для записи данных из таблицы message_actions
    @task
    def extract_message():
        q_message="""
        Select 
            user_id,
            event_date,
            messages_sent, 
            users_sent, 
            messages_received, 
            users_received,
            if(a.age = 0, b.os, a.os) AS os,
            if(a.age = 0, b.age, a.age) AS age,  
            if(a.age = 0, b.gender, a.gender) AS gender
        FROM
        (Select 
            user_id,
            event_date,
            messages_sent, 
            users_sent, 
            messages_received, 
            users_received,
            os,
            age,
            gender
        FROM
        (Select 
            user_id as user_id,
            toDate(time) as event_date,
            count(reciever_id) as messages_sent,
            count(distinct reciever_id) as users_sent,
            os,
            age,
            gender
        FROM {db}.message_actions 
        WHERE toDate(time) = today() - 1
        GROUP BY user_id,os,age,gender,toDate(time)) as tmp1

        FULL OUTER JOIN

        (Select 
            reciever_id as user_id,
            count(user_id) as messages_received,
            count(distinct user_id) as users_received 
        FROM {db}.message_actions 
        WHERE toDate(time) = today() - 1
        GROUP BY user_id,toDate(time)) as tmp2
        Using user_id) as a

        LEFT JOIN

        (SELECT DISTINCT user_id, gender, age, os FROM {db}.feed_actions) as b

        ON a.user_id = b.user_id
        """
        df2 = pandahouse.read_clickhouse(q_message, connection=connection)
        return df2
    
    @task
    #объединяем таблицы и заполняем nan нулями
    def joining(df1,df2):
        df=df1.merge(df2, how='outer', on=['event_date', 'user_id', 'gender', 'age', 'os']).fillna(0)
        return df
    
    #срез по гендеру 
    @task
    def gender_slice(df):
        df_gender = df[['gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                            .groupby('gender', as_index=False)\
                            .sum()
        
        df_gender["dimension"]="gender"
        df_gender["dimensions_value"]=df_gender["gender"]
        df_gender.drop("gender",axis=1,inplace=True)
        df_gender= df_gender.reindex(columns=['dimension', 'dimensions_value', 'views', 'likes',
                                              'messages_received','messages_sent', 'users_received', 'users_sent'])
        return df_gender
    
    #срез по os
    @task
    def os_slice(df):
        df_os=df[['os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                            .groupby('os', as_index=False)\
                            .sum()
        
        df_os["dimension"]="os"
        df_os["dimensions_value"]=df_os["os"]
        df_os.drop("os",axis=1,inplace=True)
        df_os= df_os.reindex(columns=['dimension', 'dimensions_value', 'views', 'likes', 'messages_received', 'messages_sent',
                                      'users_received', 'users_sent'])
        return df_os
    
    #срез по возрасту
    @task
    def age_slice(df):
        df_age=df[['age', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                            .groupby('age', as_index=False)\
                            .sum()
        
        df_age["dimension"]="age"
        df_age["dimensions_value"]=df_age["age"]
        df_age.drop("age",axis=1,inplace=True)
        df_age= df_age.reindex(columns=['dimension', 'dimensions_value', 'views', 'likes', 'messages_received', 'messages_sent',
                                        'users_received', 'users_sent'])
        return df_age
    #объединение срезов в одну таблицу
    @task
    def joint_all_df(df1,df2,df3):    
        df_all=pd.concat([df1,df2,df3], axis=0)

        yesterday = date.today() - timedelta(days=1)
        
        df_all["event_date"]=yesterday
        df_all= df_all.reindex(columns=['event_date','dimension', 'dimensions_value',
                                        'views', 'likes', 'messages_received','messages_sent', 'users_received', 'users_sent'])
        return(df_all)
    
    
    
    @task
    def load(df_all):
        
        df_all=df_all.astype({'event_date': 'datetime64[ns]','dimension' : "str", 'dimensions_value' : "str",
                              'views' : "int", 'likes' : "int",'messages_received' : "int",'messages_sent' : "int",
                              'users_received' :"int", 'users_sent' : "int"})
        query = '''
            CREATE TABLE IF NOT EXISTS test.abiletsky
            (
            event_date Date,
            dimension String,
            dimensions_value String,
            views Int64,
            likes Int64,
            messages_received Int64,
            messages_sent Int64,
            users_received Int64,
            users_sent Int64
            )
            ENGINE = MergeTree()
            ORDER BY event_date
            '''
        pandahouse.execute(query, connection=connection_test)
        pandahouse.to_clickhouse(df_all, 'abiletsky', index=False, connection=connection_test)
    
    
    df1=extract_feed()
    df2=extract_message()
    df=joining(df1,df2)
    df_gender=gender_slice(df)
    df_age=age_slice(df)
    df_os=os_slice(df)
    df_all=joint_all_df(df_gender,df_os,df_age)
    load(df_all)
    
arseniy_dag1_task8 = arseniy_dag1_task8()