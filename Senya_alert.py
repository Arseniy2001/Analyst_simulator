# coding=utf-8
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date, datetime, timedelta
import io
import sys
import os

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

def check_anomaly_quant(df, metric, n,a):
    
    low=[]
    up=[]
    hm=[]
    len_day=df[df["date"]==df["date"].max()].shape[0]
    for i in range(len_day):
        q_3=df.iloc[-n-len_day+i:-len_day+i,:][metric].quantile(q=0.75)
        q_1=df.iloc[-n-len_day+i:-len_day+i,:][metric].quantile(q=0.25)
        IQR=q_3-q_1
        low.append(q_1-a*IQR)
        up.append(q_3+a*IQR)
        hm.append(df.iloc[-len_day+i,:]["hm"])
    
    mean_n=df.iloc[-n-len_day+i:-len_day+i,:][metric].mean()
    df2=pd.DataFrame()
    df2["low"]=low
    df2["up"]=up
    df2["hm"]=hm
    
    df2['up'] = df2['up'].rolling(n, center=True, min_periods=1).mean()
    df2['low'] = df2['low'].rolling(n, center=False, min_periods=1).mean()

    current_ts = df['ts'].max()
    current_val = df[df['ts'] == current_ts][metric].iloc[0]
    
    if current_val>up[-1] or current_val<low[-1]:
        is_alert = 1
    else:
        is_alert = 0
 
    if current_val <= mean_n:
        diff = abs(current_val / mean_n - 1)
    else:
        diff = abs(mean_n / current_val - 1)

    return is_alert, current_val, diff, df2


def check_anomaly_sigma(df, metric, n,a):
    mean_n=[]
    std_n=[]
    low=[]
    up=[]
    hm=[]
    len_day=df[df["date"]==df["date"].max()].shape[0]
    for i in range(len_day):
        mean_n=df.iloc[-n-len_day+i:-len_day+i,:][metric].mean()
        std_n=df.iloc[-n-len_day+i:-len_day+i,:][metric].std()
        low.append(mean_n-a*std_n)
        up.append(mean_n+a*std_n)
        hm.append(df.iloc[-len_day+i,:]["hm"])

    df2=pd.DataFrame()
    df2["low"]=low
    df2["up"]=up
    
    df2["hm"]=hm
    current_ts = df['ts'].max()
    current_val = df[df['ts'] == current_ts][metric].iloc[0]

    if current_val>up[-1] or current_val<low[-1]:
        is_alert = 1
    else:
        is_alert = 0

    if current_val <= mean_n:
        diff = abs(current_val / mean_n - 1)
    else:
        diff = abs(mean_n / current_val - 1)

    return is_alert, current_val, diff, df2



# Интервал запуска DAG
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def al_task10_abiletskiy():
    
    #выгружаем данные
    @task
    def extract():
        q_f = ''' SELECT
                              toStartOfFifteenMinutes(time) as ts,
                              toDate(ts) as date,
                              formatDateTime(ts, '%R') as hm,
                              uniqExact(user_id) as users_lenta,
                              sum(action='like') as likes,
                              sum(action='view') as views,
                              likes/views as ctr

                            FROM {db}.feed_actions
                            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts '''
        q_m = ''' SELECT
                                  toStartOfFifteenMinutes(time) as ts,
                                  toDate(ts) as date,
                                  formatDateTime(ts, '%R') as hm,
                                  uniqExact(user_id) as users_mes,
                                  count(reciever_id) as messages_sent

                            FROM {db}.message_actions
                            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts '''



        data_f = pandahouse.read_clickhouse(q_f, connection=connection)
        data_m = pandahouse.read_clickhouse(q_m, connection=connection)
        df=data_f.merge(data_m, how='outer', on=['ts', 'date', 'hm'])
        return df
    
    @task
    def run_alerts(df):

        my_token = '6674887967:AAGK3YeyOYhBkiE4fAAHkQI-vT1vzOX7rxA'
        bot = telegram.Bot(token=my_token) # получаем доступ
        chat_id=-928988566
        metric_mas=["users_lenta","likes","views","ctr","users_mes","messages_sent"]

        is_alert=0
        for metric in metric_mas:

            #метрика ctr имеет нормальное распределение, поэтому используем правило трех сигм
            if metric == "ctr":
                is_alert, current_value, diff,df2 = check_anomaly_sigma(df, metric, n=16,a=3)   

            #для остальных метрик используем межквантильный размах
            elif metric=="users_mes" or metric=="messages_sent":
                is_alert, current_value, diff,df2 = check_anomaly_quant(df, metric, n=4,a=3.5)

            else:
                is_alert, current_value, diff,df2 = check_anomaly_quant(df, metric, n=3,a=5)    


            if is_alert:

                msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от среднего {diff:.2%}'''.format(metric=metric,current_value=current_value, diff=diff)
                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()

                ax = sns.lineplot(data=df[df["date"]==df["date"].max()],x="hm", y=metric)
                ax = sns.lineplot(data=df2,x="hm",y="low")
                ax = sns.lineplot(data=df2,x="hm",y="up")

                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У

                ax.set_title('{}'.format(metric)) # задае заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У

                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
     

    df=extract()
    run_alerts(df)
al_task10_abiletskiy=al_task10_abiletskiy()