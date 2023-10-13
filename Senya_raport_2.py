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

def printp(new,old):
    new=float(new)
    old=float(old)
    result=(new-old)/old*100
    text_print=""
    if result>0:
        text_print="Рост "+str(round(result,2))+" % по сравнению с прошлой неделей"
    elif result<0:
        text_print="Падение "+str(round(result,2))+" % по сравнению с прошлой неделей"
    else:
        text_print="Нет изменений по сравнению с прошлой неделей"
    return text_print



# Интервал запуска DAG
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def raport_task9_2_abiletskiy():
    
    #выгружаем данные
    @task
    def sending():

        #DAU
        q_dau = """

        SELECT
            Date,
            count_user_m,
            count_user_f
        FROM
        (SELECT 
            toDate(time) as Date,
            count(DISTINCT user_id) as count_user_m

        FROM {db}.message_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9
        GROUP BY toDate(time)) as a

        FULL OUTER JOIN

        (SELECT 
            toDate(time) as Date,
            count(DISTINCT user_id) as count_user_f

        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9
        GROUP BY toDate(time)) as b

        ON a.Date=b.Date
        """

        q_dau_all = """

        SELECT
            Date,
            count(DISTINCT user_id) as count_user
        FROM
        (SELECT DISTINCT user_id, toDate(time) as Date
        FROM {db}.message_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9) as a

        FULL OUTER JOIN

        (SELECT DISTINCT user_id, toDate(time) as Date

        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9) as b


        USING Date,user_id
        GROUP BY Date
        """
        #DAU source
        q_source = """

        SELECT
            Date,
            count(DISTINCT user_id) as count_user,
            source
        FROM
        (SELECT DISTINCT user_id, 
            toDate(time) as Date,
            source
        FROM {db}.message_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9) as a

        FULL OUTER JOIN

        (SELECT DISTINCT user_id,
            toDate(time) as Date,
            source 
        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9) as b


        USING Date,user_id,source
        GROUP BY Date,source
        ORDER BY Date
        """

        #Action
        q_action_f = """

        SELECT 
            toDate(time) as Date,
            count(action) as actions 
        FROM {db}.feed_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9
        GROUP BY Date
        ORDER BY Date
        """

        q_action_m = """

        SELECT   
            toDate(time) as Date,
            count(reciever_id) as actions
        FROM {db}.message_actions 
        WHERE toDate(time) < today() and toDate(time) > today() - 9
        GROUP BY Date
        ORDER BY Date
        """

        #new user
        q_new_user_f = """
        SELECT start_date as date, COUNT(user_id ) as new_user
        FROM 
            (SELECT  user_id, min(toDate(time)) as start_date 
            FROM simulator_20230820.feed_actions 
            GROUP BY user_id) as t1
        GROUP BY start_date
        HAVING start_date>today()-9 and start_date < today()
        """
        q_new_user_m = """
        SELECT start_date as date, COUNT(user_id ) as new_user
        FROM 
            (SELECT  user_id, min(toDate(time)) as start_date 
            FROM simulator_20230820.message_actions 
            GROUP BY user_id) as t1
        GROUP BY start_date
        HAVING start_date>today()-9 and start_date < today()
        """

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



        # DAU текст
        df1 = pandahouse.read_clickhouse(q_dau, connection=connection)
        df2 = pandahouse.read_clickhouse(q_dau_all, connection=connection)

        df1=df1.astype({'Date': 'datetime64[ns]'})
        df2=df2.astype({'Date': 'datetime64[ns]'})

        df1["count_user"]=df2["count_user"]

        yesterday_dau_m=df1.iloc[-1,1]
        one_week_dau_m=df1.iloc[0,1]

        yesterday_dau_f=df1.iloc[-1,2]
        one_week_dau_f=df1.iloc[0,2]

        yesterday_dau=df1.iloc[-1,3]
        one_week_dau=df1.iloc[0,3]

        DAU="\nDAU (all): "+str(yesterday_dau)+"\n"+printp(yesterday_dau,one_week_dau)+"\n\n"
        DAU_f="DAU (feed): "+str(yesterday_dau_f)+"\n"+printp(yesterday_dau_f,one_week_dau_f)+"\n\n"
        DAU_m="DAU (message): "+str(yesterday_dau_m)+"\n"+printp(yesterday_dau_m,one_week_dau_m)+"\n"
        Mes1="<b>DAU</b>\n"+DAU+DAU_f+DAU_m+"\n__________________________\n\n"

        df1=df1[df1["Date"]>df1["Date"].min()]

        #DAU source text
        df3 = pandahouse.read_clickhouse(q_source, connection=connection)
        df3=df3.astype({'Date': 'datetime64[ns]'})

        yesterday_dau_a=df3[df3["source"]=="ads"].iloc[-1,1]
        one_week_dau_a=df3[df3["source"]=="ads"].iloc[0,1]

        yesterday_dau_o=df3[df3["source"]=="organic"].iloc[-1,1]
        one_week_dau_o=df3[df3["source"]=="organic"].iloc[0,1]

        DAU_by_source_a="\nDAU by sources(ads): "+str(yesterday_dau_a)+"\n"+printp(yesterday_dau_a,one_week_dau_a)+"\n\n"
        DAU_by_source_o="DAU by sources(organic): "+str(yesterday_dau_o)+"\n"+printp(yesterday_dau_o,one_week_dau_o)+"\n"


        Mes2="<b>DAU by sources for feed and message</b>\n"+DAU_by_source_a+DAU_by_source_o+"\n__________________________\n\n"

        df3=df3[df3["Date"]>df3["Date"].min()]


        #Action text
        df_f = pandahouse.read_clickhouse(q_action_f, connection=connection)
        df_f=df_f.astype({'Date': 'datetime64[ns]'})
        df_m = pandahouse.read_clickhouse(q_action_m, connection=connection)
        df_m=df_m.astype({'Date': 'datetime64[ns]'})


        df_f = pandahouse.read_clickhouse(q_action_f, connection=connection)

        df_m = pandahouse.read_clickhouse(q_action_m, connection=connection)

        yesterday_action_f=df_f.iloc[-1,1]
        one_week_action_f=df_f.iloc[0,1]

        yesterday_action_m=df_m.iloc[-1,1]
        one_week_action_m=df_m.iloc[0,1]

        Actions_f="\nActions (likes+views): "+str(yesterday_action_f)+"\n"+printp(yesterday_action_f,one_week_action_f)+"\n\n"
        Actions_m="Actions (messages sent): "+str(yesterday_action_m)+"\n"+printp(yesterday_action_m,one_week_action_m)+"\n"
        Mes3="<b>Actions for feed and messages</b>\n"+Actions_f+Actions_m+"\n__________________________\n\n"

        df_f=df_f[df_f["Date"]>df_f["Date"].min()]
        df_m=df_m[df_m["Date"]>df_m["Date"].min()]

        #new user text

        df_newuser_f=pandahouse.read_clickhouse(q_new_user_f, connection=connection)
        df_newuser_m=pandahouse.read_clickhouse(q_new_user_m, connection=connection)

        yesterday_newu_f=df_newuser_f.iloc[-1,1]
        one_week_newu_f=df_newuser_f.iloc[0,1]

        yesterday_newu_m=df_newuser_m.iloc[-1,1]
        one_week_newu_m=df_newuser_m.iloc[0,1]

        New_users_f="\nNew users (feed): "+str(yesterday_newu_f)+"\n"+printp(yesterday_newu_f,one_week_newu_f)+"\n\n"
        New_users_m="New users (messages): "+str(yesterday_newu_m)+"\n"+printp(yesterday_newu_m,one_week_newu_m)+"\n"

        Mes4="<b>New users</b>\n"+New_users_f+New_users_m+"\n__________________________\n\n"

        df_f=df_f[df_f["Date"]>df_f["Date"].min()]
        df_m=df_m[df_m["Date"]>df_m["Date"].min()]


        #CTR text
        df_ctr=pandahouse.read_clickhouse(q_ctr, connection=connection)

        yesterday_ctr=round(df_ctr.iloc[-1,3],4)
        one_week_ctr=round(df_ctr.iloc[0,3],4)


        Ctr="CTR: "+str(yesterday_ctr)+"\n"+printp(yesterday_ctr,one_week_ctr)+"\n"


        Mes5="<b>Feed</b>\n\n"+Ctr+"\n__________________________\n\n"

        #All text
        yesterday = date.today() - timedelta(days=1)
        Mes="<b>Данные за</b> "+str(yesterday)+" <b>по всему приложению</b>\n\n"+Mes1+Mes2+Mes3+Mes4+Mes5

        #DAU plot
        sns.set(rc={"figure.figsize":(15,10)})
        sns.set(rc={'axes.facecolor':'lightgrey', 'figure.facecolor':'white'})
        plt.title("DAU")
        sns.lineplot(x=df1.Date,y=df1.count_user, marker='o',label="DAU all")
        sns.lineplot(x=df1.Date,y=df1.count_user_f, marker='o',label="DAU feed")
        sns.lineplot(x=df1.Date,y=df1.count_user_m, marker='o',label="DAU message")
        plt.ylabel("number of users")
        plot_object=io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name="DAU.png"
        media_array=[]
        media_array.append(telegram.InputMediaPhoto(plot_object))
        plt.close()

        #DAU source plot
        plt.title("DAU by sources")
        sns.lineplot(x=df3.Date,y=df3.count_user,hue=df3.source, marker='o')
        plt.ylabel("number of users")
        plot_object2=io.BytesIO()
        plt.savefig(plot_object2)
        plot_object2.seek(0)
        plot_object2.name="DAU_by_source.png"
        media_array.append(telegram.InputMediaPhoto(plot_object2))
        plt.close()

        #Action plot
        fig, axs = plt.subplots(2,1,figsize=(15, 15))
        plt.title("Actions")
        sns.lineplot(x=df_f.Date,y=df_f.actions, marker='o',label="action(like+view)",ax=axs[0],color="purple")
        sns.lineplot(x=df_m.Date,y=df_m.actions, marker='o',label="action(messages sent)",ax=axs[1],color="blue")
        plt.ticklabel_format(style='plain', axis='y', useOffset=False)
        plot_object3=io.BytesIO()
        plt.savefig(plot_object3)
        plot_object3.seek(0)
        plot_object3.name="Action.png"
        media_array.append(telegram.InputMediaPhoto(plot_object3))
        plt.close()

        #отправляем все боту
        my_token = '6674887967:AAGK3YeyOYhBkiE4fAAHkQI-vT1vzOX7rxA'
        bot = telegram.Bot(token=my_token) # получаем доступ

        # chat_id=345715010        
        chat_id=-928988566

        #отправляем сообщения
        bot.sendMessage(chat_id=chat_id,text=Mes, parse_mode="html")


        #отправляем 3 графика
        bot.sendMediaGroup(chat_id=chat_id, media=media_array)
        

    sending()
raport_task9_2_abiletskiy=raport_task9_2_abiletskiy()