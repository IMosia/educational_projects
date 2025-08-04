import pandas as pd
import pandahouse as pandahouse
from io import StringIO
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task

def ch_get_df(query,
              host='',
              user='',
              password=''):
    """
    Function to connect to ClickHouse and run query
    Input:  query - SQL string for request
            default connection information
    Output: pandas dataframe with query results
    """
    
    r = requests.post(host,
                      data = query.encode("utf-8"),
                      auth=(user, password),
                      verify=False)
    
    result = pd.read_csv(StringIO(r.text), sep='\t')
    
    return result

# Deafault parametrs for airflow DAGs
default_args = {
        'owner': 'i.mosiagin',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'start_date': datetime(2024, 1, 11),
        }

# Interval for DAG calling
# Не будем ставить расчёт на 1 ночи, чтобы не будить наших бедных аналитиков:) 
# Пусть лучше анализ приходит перед началом рабочего дня
schedule_interval = '0 7 * * *'

@dag(default_args = default_args,
     schedule_interval = schedule_interval,
     catchup = False)
def dag_etl_imosia():
    """
    DAG for ETL pipeline
    1. Download info from tables feed_actions & user_actions with partition on users
        Info: number of likes, view, messages sent and recieved, number of connection (to and from user)
    2. Join both tables
    3. Calculate grouped metrics with grouping on gender, age, and os used (three separate tasks)
    4. Combined table should be uploaded to ClickHouse (name: test.imosia_etl_prod)
    5. Run every day for the past day (update info)
    
    Final table cotnatins: event_date, dimension (gender, os, age), dimension_value, views, likes,
                            messages_received, messages_sent, users_received, users_sent
    """
    
    @task()
    def extract_feed_actions():
        """
        Download data from feed_actions
        """
        query_feed_actions = """
            SELECT
                user_id
                , toDate(time) as event_date
                , gender
                , age
                , os 
                , SUM (if(action = 'view', 1, 0)) as views
                , SUM (if(action = 'like', 1, 0)) as likes
            FROM simulator_20231220.feed_actions
            WHERE toDate(time) = today() - 1
            GROUP BY user_id, toDate(time), gender, age, os
            format TSVWithNames
        """
        
        df = ch_get_df(query = query_feed_actions)
        return df

    @task()
    def extract_message_actions():
        """
        To download data from message_actions
        """
        query_message_actions = """
            SELECT 
                COALESCE(user_id, receiver_id) AS user_id
                , COALESCE(sent.event_date, recived.event_date) as event_date
                , gender
                , age
                , os
                , messages_received
                , messages_sent
                , users_received
                , users_sent
            FROM (
                SELECT 
                    user_id, toDate(time) as event_date,
                    gender, age, os, 
                    count (receiver_id) as messages_sent,
                    count (distinct receiver_id) as users_sent
                FROM simulator_20231220.message_actions
                WHERE toDate(time) = today() - 1
                GROUP BY user_id, toDate(time), gender, age, os
                ) sent
                LEFT JOIN
                (SELECT
                    receiver_id, toDate(time) as event_date,  
                    count (user_id) as messages_received,
                    count (distinct user_id) as users_received
                FROM simulator_20231220.message_actions
                WHERE toDate(time) = today() - 1
                GROUP BY receiver_id, toDate(time) 
                ) recived
                ON sent.user_id = recived.receiver_id
            format TSVWithNames
        """
        
        df = ch_get_df(query = query_message_actions)
        return df

    @task()
    def join_feed_and_message(df_feed, df_message):
        """
        Combine two dataframes from tables feed and message
        and cleans the data
        Input: df_feed, df_message - startig dataframes
        Output: df_combined - combined and cleaned dataframe
        """
        
        df_combined = df_feed.merge(df_message, how = 'outer', on = ['user_id', 'event_date', 'os', 'gender', 'age'])
        
        # let's clean the combined dataframe a bit:
        # if there is nothing in second dataframe, it is probably 0
        df_combined['views'].fillna(0, inplace=True)
        df_combined['likes'].fillna(0, inplace=True)
        df_combined['messages_received'].fillna(0, inplace=True)
        df_combined['messages_sent'].fillna(0, inplace=True)
        df_combined['users_received'].fillna(0, inplace=True)
        df_combined['users_sent'].fillna(0, inplace=True)
  
        # if we do not known information to group by, let's indicate it
        df_combined['os'].fillna('unknown', inplace=True)
        df_combined['gender'].fillna(-1, inplace=True)
        
        age_intervals = [0, 18, 25, 35, 45, 60, 100]
        age_labels = ['0-18', '19-25', '26-35', '36-45', '46-60', '61+']
        df_combined['age_group'] = pd.cut(df_combined['age'], bins=age_intervals, labels=age_labels, right=False)
        df_combined = df_combined.drop('age', axis=1)
        df_combined = df_combined.rename(columns={'age_group': 'age'})
        df_combined['age'] = df_combined['age'].cat.add_categories(['unknown'])
        df_combined['age'].fillna('unknown', inplace=True)
        
        return df_combined

    @task
    def transfrom_by_group(df, name):
        """
        Takes dataframe, combines it by selected parametr and calculate sum
        Input: dataframe,
                name - name of column to proces
        Output: dataframe with calculate sum statistics
        """
        grouped_df = df[['event_date', name, 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 
                         'users_sent']].groupby(['event_date', name]).sum().reset_index()
        
        grouped_df['dimension'] = name
        grouped_df = grouped_df.rename(columns = {name: 'dimension_value'}).astype('str')
        
        return grouped_df


    @task
    def combine_tables(df_gender, df_os, df_age):
        """
        Task to merge all the dataframes together
        Input: 3 dataframes with partitioning on gender, os, and age
        Output: combined dataframe with the correct columns order
        """
        
        df_full = pd.concat([df_gender, df_os, df_age], ignore_index=True)
        
        return df_full[['event_date', 'dimension', 'dimension_value', 'views', 'likes',
                         'messages_received', 'messages_sent', 'users_received', 'users_sent']]

    
    @task
    def upload_restults_to_click_house(df):
        """
        Function to upload results to Click House
        Takes df_full with all the data, checks that table is existing (or crestes it),
                and adds data for the new day
        Input: df with all the information
        Output: None - uploads data to table in Click House
        """
        
        upload_connection = {
            'host': '',
            'database': '',
            'password': '',
            'user': ''
        }
        
        create_table = '''
            CREATE TABLE IF NOT EXISTS test.imosia_etl_prod
            (
                event_date Date,
                dimension String,
                dimension_value String,
                views Nullable(double),
                likes Nullable(double),
                messages_received Nullable(double),
                messages_sent Nullable(double),
                users_received Nullable(double),
                users_sent Nullable(double)
            )
            ENGINE = MergeTree()
            ORDER BY (event_date, dimension, dimension_value)
            PRIMARY KEY (event_date, dimension, dimension_value)
        '''

        pandahouse.execute(query = create_table, connection = upload_connection)
        
        pandahouse.to_clickhouse(df = df, table = 'imosia_etl_prod',
                                 index = False, connection = upload_connection)

        
         
            
    df_feed = extract_feed_actions()
    df_message = extract_message_actions()
    df_combined = join_feed_and_message(df_feed, df_message)
    
    df_gender = transfrom_by_group(df_combined, 'gender')
    df_os = transfrom_by_group(df_combined, 'os')
    df_age = transfrom_by_group(df_combined, 'age')
    
    df_full = combine_tables(df_gender, df_os, df_age)
    upload_restults_to_click_house(df_full)

dag_etl_imosia = dag_etl_imosia()
