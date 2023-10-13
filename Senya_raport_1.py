# coding=utf-8
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context




# подключение к базе для считывания
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230820',
              'user':'student',
              'password':'dpo_python_2020'}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-biletskij',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 26),
}


# Интервал запуска DAG
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def raport_task9_abiletskiy():
    
    #выгружаем данные
    @task
    def sending():
        q_ctr = """
        SELECT 
            toDate(time) as Date,
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            likes/views as ctr

        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 8
        GROUP BY toDate(time)
        """

        q_dau = """
        SELECT 
            toDate(time) as Date,
            count(DISTINCT user_id) as count_user

        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 8
        GROUP BY toDate(time)
        """

        df1 = pandahouse.read_clickhouse(q_ctr, connection=connection)
        df2 = pandahouse.read_clickhouse(q_dau, connection=connection)
        #настройка типа для более красивого отображения на графике
        df1=df1.astype({'Date': 'datetime64[ns]'})
        df2=df2.astype({'Date': 'datetime64[ns]'})
    
        #создаем текст и графики

        
        #вычисляем метрики за вчерашний день 
        
        yesterday_ctr=df1.iloc[-1,3]
        yesterday_likes=df1.iloc[-1,1]
        yesterday_views=df1.iloc[-1,2]
        yesterday_dau=df2.iloc[-1,1]
        
        # вчерашняя дата
        yesterday = date.today() - timedelta(days=1)
        
        #текст сообщений для бота
        mas="<b>Значения метрик за вчерашний день</b>("+str(yesterday)+")\nDAU: "+str(yesterday_dau)+"\n"+"CTR: "+str(round(yesterday_ctr,3))+"\n"+"Лайки: "+str(yesterday_likes)+"\n"+"Просмотры: "+str(yesterday_views)
        mas2="<b>Графики за предыдущие 7 дней</b>"

        
        #задаем размер окна
        sns.set(rc={'figure.figsize':(12,9)})
        
        #график dau
        sns.lineplot(x=df2.Date,y=df2.count_user)
        plt.ylabel('number of users')
        plt.title("DAU")
        plot_object=io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name="DAU.png"
        plt.close()

        #график ctr       
        sns.lineplot(x=df1.Date,y=df1.ctr)
        plt.title("CTR")
        plot_object4=io.BytesIO()
        plt.savefig(plot_object4)
        plot_object4.seek(0)
        plot_object4.name="CTR.png"
        plt.close()        
        
        #график likes
        sns.lineplot(x=df1.Date,y=df1.likes)
        plt.title("Likes")
        plot_object2=io.BytesIO()
        plt.savefig(plot_object2)
        plot_object2.seek(0)
        plot_object2.name="Likes.png"
        plt.close()
       

        #график views
        sns.lineplot(x=df1.Date,y=df1.views)
        plt.title("Views")
        plot_object3=io.BytesIO()
        plt.savefig(plot_object3)
        plot_object3.seek(0)
        plot_object3.name="Views.png"
        plt.close()
        

        
        #отправка данных боту

        my_token = '6674887967:AAGK3YeyOYhBkiE4fAAHkQI-vT1vzOX7rxA'
        bot = telegram.Bot(token=my_token) # получаем доступ

        
        chat_id=-928988566
        
        #отправляем сообщения
        bot.sendMessage(chat_id=chat_id,text=mas, parse_mode="html")
        bot.sendMessage(chat_id=chat_id,text=mas2, parse_mode="html")
        
        #отправляем 4 графика
        bot.sendPhoto(chat_id=chat_id,photo=plot_object)
        bot.sendPhoto(chat_id=chat_id,photo=plot_object4)      
        bot.sendPhoto(chat_id=chat_id,photo=plot_object2)
        bot.sendPhoto(chat_id=chat_id,photo=plot_object3)
        

    sending()
raport_task9_abiletskiy=raport_task9_abiletskiy()