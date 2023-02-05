from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import requests
import pandas as pd
import pandahouse

# Перед созданием DAG'а на питоне в Airflow необходимо импортировать нужные библиотеки

default_args = {'owner': 'i-gromov',  
                'depends_on_past': False,
                'retries': 2, 
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 1, 12)}

# Создадим словарь, в котором укажем основные параметры работы DAG'а в Airflow

schedule_interval = '0 18 * * *'

connection1 = {'host': 'https://clickhouse.lab.karpov.courses',
               'password': '656e2b0c9c',
               'user': 'student-rw',
               'database': 'test'}

# Также созданим словарь, который будет нужен при запросаз для подключения к базе данных в Clickhouse             

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def i_gromov_dag_task_6():
    @task()
    def extract_1_1():
        connection2 = {'host': 'https://clickhouse.lab.karpov.courses',
                       'password': 'dpo_python_2020',
                       'user': 'student',
                       'database': 'simulator_20221220'}
        query1 = """
        SELECT toDate(time) AS event_date, 
            user_id AS user, 
            countIf(action='like') AS likes, 
            countIf(action='view') AS views, 
            IF(gender=0,'female','male') AS  gender, 
            age, 
            os
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) = today() - 1
        GROUP BY event_date, user_id, gender, age, os"""
        df1 = pandahouse.read_clickhouse(query1, connection=connection2)
        return df1

# В данном таске был сформирован датафрейм после запроса к базе данных, в котором отражены значения лайков, просмотров сгруппированные по гендеру, дате и операционной системе

    @task()
    def extract_1_2():
        connection2 = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20221220'
        }
        query2 = """
        SELECT event_date, user, messages_sent, users_sent, messages_received, users_received, gender, age, os FROM 
        (SELECT * FROM
        (SELECT toDate(time) AS event_date, 
            user_id AS user,
            COUNT(reciever_id) AS messages_sent, 
            COUNT(DISTINCT reciever_id) AS users_sent, 
            IF(gender=0,'female','male') AS  gender,
            age,
            os
        FROM simulator_20221220.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY event_date, user, gender, age, os) AS z1
        JOIN
        (SELECT event_date, user, messages_received, users_received, gender, age, os
        FROM
        (SELECT DISTINCT user_id AS user,
        IF(gender=0, 'female', 'male') AS gender,
            age,
            country,
            os
        FROM simulator_20221220.message_actions) AS z2
        JOIN
        (SELECT toDate(time) AS event_date,
            reciever_id AS user,
            COUNT(user_id) AS messages_received,
            COUNT(DISTINCT user_id) AS users_received
        FROM simulator_20221220.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY event_date, user) AS z3
        USING user) AS z4
        USING user) AS z5"""
        df2 = pandahouse.read_clickhouse(query2, connection=connection2)
        return df2

# В следующем таске также делается запрос к базе данных, но в нем отражается количество отправленных и полученных сообщений, сгруппированных также по полу, дате и операционной системе

    @task()
    def transform_2(df1, df2):
        df3 = df2.merge(df1, 
            how='outer', 
            on=['user', 'event_date', 'age', 'os',  'gender'], 
            right_index=False)
        return df3

# Далее следует преобразование двух датафреймов в один

    @task()
    def transform_3_1(df3):
        df4 = df3.groupby(['event_date', 'gender'])['views','likes','messages_sent','users_sent','messages_received','users_received'].sum().reset_index()
        df4['dimension'] = 'gender'
        df4 = df4.rename(columns={'gender': 'dimension_value'})
        return df4
    @task()
    def transform_3_2(df3):
        df5 = df3.groupby(['event_date', 'age'])['views','likes','messages_sent','users_sent','messages_received','users_received'].sum().reset_index()
        df5['dimension'] = 'age'
        df5 = df5.rename(columns={'age': 'dimension_value'})
        return df5
    @task()
    def transform_3_3(df3):
        df6 = df3.groupby(['event_date', 'os'])['views','likes','messages_sent','users_sent','messages_received','users_received'].sum().reset_index()
        df6['dimension'] = 'os'
        df6 = df6.rename(columns={'os': 'dimension_value'})
        return df6

# После идут таски по преобразованию полученного датафрейма, а именно группировки по одному из трех нужных срезов и создания трех датафреймов

    @task()
    def transform_4(df4, df5, df6):
        df7 = pd.concat([df4, df5, df6], 
            ignore_index = True)
        df7 = df7.astype({'views': 'int32','likes': 'int32','messages_sent': 'int32','users_sent': 'int32','messages_received' : 'int32','users_received' : 'int32'})
        return df7

# Здесь происходит объединение методом concat всех трех датафреймов, а также задание необходимого типа для данных для дальнейшего преобразования

    @task()
    def load_5(df7):
        query3 = """
        CREATE TABLE IF NOT EXISTS test.i_gromov(
                                            event_date String,
                                            dimension String,
                                            dimension_value String,
                                            views Int32,
                                            likes Int32,
                                            messages_sent Int32,
                                            users_sent Int32,
                                            messages_received Int32,
                                            users_received Int32)
                                            ENGINE = MergeTree()
                                            ORDER BY event_date"""
        pandahouse.execute(query = query3, connection=connection1)
        pandahouse.to_clickhouse(df = df7, table='i_gromov', index=False, connection=connection1)

 # Заключительный таск по созданию таблицы в Clickhouse        
    
    df1 = extract_1_1()
    df2 = extract_1_2()
    df3 = transform_2(df1, df2)
    df4 = transform_3_1(df3)
    df5 = transform_3_2(df3)
    df6 = transform_3_3(df3)
    df7 = transform_4(df4, df5, df6)   
    load_5(df7)
    
i_gromov_dag_task_6 = i_gromov_dag_task_6()