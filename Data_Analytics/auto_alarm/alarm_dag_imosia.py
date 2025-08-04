import pandas as pd
import pandahouse as pandahouse
import io
import requests
from datetime import datetime, timedelta, date
import telegram 
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from airflow.decorators import dag, task

# Telegram info
my_token = ''
my_own_chat_id = 
group_chat_id = 
chat_id_list = [my_own_chat_id, group_chat_id]

# Deafault parametrs for airflow DAGs
default_args = {
        'owner': 'i.mosiagin',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'start_date': datetime(2024, 1, 13),
        }

# Interval for DAG calling every 15 minutes
schedule_interval = '*/15 * * * *'

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
    
    result = pd.read_csv(io.StringIO(r.text), sep='\t')
    return result

def inform_if_something_is_wrong(stats, value, median, name, alread_anomaly):
    """
    Function takes information that something is wrong
    Prepares alarm and sends it
    """
    if alread_anomaly:
        for chat_id in chat_id_list:
            message = f'Also pay attention to {name}\n'
            message += f'''Current value: {value}, median: {median}, difference = {(100*(value - median)/median):.2f}%'''
            bot = telegram.Bot(token=my_token)
            bot.sendMessage(chat_id=chat_id, text=message)

    else:
        for chat_id in chat_id_list:
            if value > median:
                message = f'''{name} metrics is unusually high!\n'''
            else:
                message = f'''ALARM!!!\n{name} METRICS IS DANGEROUSLY LOW!\n'''
                            
            message += f'''Current value: {value}, median: {median}, difference = {(100*(value - median)/median):.2f}%'''
            
            plt.figure(figsize=(8, 6))
            plt.boxplot(stats)
            plt.title(f'Boxplot for {name}')
            plt.ylabel('Value')
            plt.scatter(1, value, color='red',
                        s=100, zorder=3, label='current_value') 
            
            graph = io.BytesIO()
            plt.savefig(graph)
            graph.seek(0)
            graph.name = f'{name}_metrics.png'
            plt.close()
            
            bot = telegram.Bot(token=my_token)
        
            bot.sendMessage(chat_id=chat_id, text=message)
            bot.sendPhoto(chat_id=chat_id, photo=graph)
    
def check_data_for_anomaly(stats, current, alpha_value):
    """
    Function to calculate met statistics on dataframe
    and predict if current value is out if whiskers
    alpha_value depends on column to analyse
    Preliminary analysis showed that in those
    conditions it will be around 2-4 alarms per week
    
    """
    median = np.median(stats)
    q_1 = np.percentile(stats, 25)
    q_3 = np.percentile(stats, 75)
    iqr = q_3 - q_1
    
    low_threshold = q_1 - alpha_value * iqr
    high_threshold = q_3 + alpha_value * iqr
    
    if (current > high_threshold) or (current < low_threshold):
        return True, median
    return False, median
   
def proceed_row_data_from_query(df):
    """
    Takes row data over last 8 days and proceeds it
    Takes dataframe with all the information,
    extracts current value and value over last
    1 week (+- 1 hours from now)
    """
    df['time_window'] = pd.to_datetime(df['time_window'])
    
    current_value = df.iloc[0]
    current_time =  current_value['time_window']
    # Takes information of metrics +- 1 hour over the last 1 week
    current_hour_window = (current_time - timedelta(hours=1), current_time + timedelta(hours=1))
    current_day_rows = df[(df['time_window'] >= current_hour_window[0])
                      & (df['time_window'] < current_value['time_window'])
                      & (df['time_window'].dt.date == current_time.date())]

    past_days_rows = pd.DataFrame(columns=df.columns)
    for i in range(1, 8):
        start_date = current_time.date() - timedelta(days=i)
        end_date = start_date + timedelta(days=1)

        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.min.time())

        day_rows = df[(df['time_window'] >= start_datetime)
                      & (df['time_window'] < end_datetime)
                      & ((df['time_window'].dt.hour == current_time.hour) |
                         (df['time_window'].dt.hour == current_time.hour + 1) |
                         (df['time_window'].dt.hour == current_time.hour - 1))]

        past_days_rows = pd.concat([past_days_rows, day_rows], ignore_index=True)
        
    statistics_df = pd.concat([current_day_rows, past_days_rows], ignore_index=True)
    current_value = df.iloc[[0]]

    return (current_value, statistics_df)

def get_info_from_feed_actions():
    """
    Function takes information from ClickHouse DB
    Containing information on actions and metrics
    over 15 minutes intervals in the last 8 days
    Input: None
    Output: pandas dataframe with aggreageted information
    """

    query_get_info_from_feed = """
        SELECT 
            toStartOfHour(toDateTime(time)) 
            + toIntervalSecond(floor(toUInt16(toMinute(toDateTime(time))) / 15) * 15 * 60)
            AS time_window,
            count(DISTINCT user_id) AS unique_users_feed,
            countIf(action = 'like') AS Likes,
            countIf(action = 'view') AS Views
        FROM simulator_20231220.feed_actions
        WHERE toDateTime(time) > now() - 8 * 24 * 60 * 60
            AND toDateTime(time) <=  now() - 15 * 60
        GROUP BY time_window
        ORDER BY time_window DESC
        FORMAT TSVWithNames
    """

    # Dataframe clean up
    df = ch_get_df(query = query_get_info_from_feed)
    function_answer = proceed_row_data_from_query(df)
    current_value, statistics_df = function_answer[0], function_answer[1]

    return (current_value, statistics_df)

def get_info_from_message():
    """
    Takes information on message actions from
    ClickHouse DB, extract statistics and 
    and saves it as pandas df for futher analysis
    Input: None
    Output: pandas dataframe
    """

    get_info_from_message_table = """
                    SELECT time_window,
                        count(distinct user_id) as unique_users_messages,
                        SUM(messages_sent) as total_messages_sent
                    FROM (
                        SELECT 
                            user_id,
                            toStartOfHour(toDateTime(time)) 
                            + toIntervalSecond(floor(toUInt16(toMinute(toDateTime(time))) / 15) * 15 * 60)
                            AS time_window,
                            count (receiver_id) as messages_sent
                        FROM simulator_20231220.message_actions
                        WHERE toDateTime(time) > now() - 8 * 24 * 60 * 60
                            AND toDateTime(time) <=  now() - 15 * 60
                        GROUP BY user_id, time_window
                    ) 
                    GROUP BY time_window
                    ORDER BY time_window DESC
                    format TSVWithNames
    """

    # Dataframe clean up
    df = ch_get_df(query = get_info_from_message_table)
    function_answer = proceed_row_data_from_query(df)
    current_value, statistics_df = function_answer[0], function_answer[1]

    return (current_value, statistics_df)

def query_for_information_by_partitions():
    """
    Function to take information by partitions
    """
    query_to_select_data = """
        SELECT  toStartOfHour(toDateTime(time)) 
                + toIntervalSecond(floor(toUInt16(toMinute(toDateTime(time))) / 15) * 15 * 60)
                AS time_window,
                user_id,
                gender,
                os,
                country,
                city,
                source,
                age
        FROM simulator_20231220.feed_actions
        WHERE toDateTime(time) > now() - 2 * 60 * 60
                    AND toDateTime(time) <=  now() - 15 * 60
        format TSVWithNames
    """

    df = ch_get_df(query_to_select_data)
    return df

def query_analyse_partitions(df, column_name):
    """
    Function to analyse if ratio of partition is lower
    then usuall 
    Input: dataframe with information
            column_name - name of column to analyse
    Output: something_is_wrong - boolean
            message - related message
    """
    something_is_wrong = False
    message = 'Ratio of the following groups has droped singifically:\n'

    if column_name == 'age':
        age_intervals = pd.cut(
            df['age'],
            bins=[0, 18, 25, 35, 55, float('inf')],
            right=False,
            labels=['0-18', '19-25', '26-35', '36-55', '56+']
        )
        df['age_interval'] = age_intervals
        column_name = 'age_interval'
    
    if column_name in ['country', 'city']:
        valid_groups = df.groupby(column_name)['user_id'].nunique() > 250
        valid_groups = valid_groups[valid_groups].index
        df = df[df[column_name].isin(valid_groups)]

    activity_data = df.groupby(['time_window', column_name])['user_id'].nunique().unstack(fill_value=0)
    total_activity = df.groupby('time_window')['user_id'].nunique()
    activity_data['total_activity'] = activity_data.sum(axis=1)

    for group_col in activity_data.columns[:-1]:
        ratio_col = f'ratio_{group_col}'
        activity_data[ratio_col] = 100 * round(activity_data[group_col] / total_activity, 4)
    
    activity_data = activity_data.rename_axis(index=None, columns=None)
    last_interval = activity_data.index.max()
    
    for col_iteration in activity_data.columns:
        col = str(col_iteration)
        if 'ratio' not in col:
            continue
            
        avg_others = activity_data[activity_data.index < last_interval][col].mean()
        difference = 100 * (activity_data[col][last_interval] - avg_others) / avg_others
        if avg_others < 5 or 'country' in col or 'city' in col:
              if difference < - 95:
                    something_is_wrong = True
                    message += f'''{col} in {column_name} has difference to avg
                        of {round(difference, 2)} %, value = {round(activity_data[col][last_interval], 2)}\n'''
        else:
            if difference < - 50:
                something_is_wrong = True
                message += f'''{col} in {column_name} has difference to avg
                    of {round(difference, 2)} %, value = {round(activity_data[col][last_interval], 2)}\n'''

    return something_is_wrong, message


@dag(default_args = default_args,
     schedule_interval = schedule_interval,
     catchup = False)
def dag_alarm_imosia():
    """
    DAG for alarms
    1. Takes information from feed and message tables
    2. Clean and selects data:
        15 minutes intervals
        stats over last week for +- h interval to now
        Metrcis: Likes, Views, Unique users of feed,
        Unique users of messages, messages sent
    3. Analyse if current value for each of the metric
        is outside of whiskers
    4. If yes - sends alarm to Telegram
    """
    @task
    def run_checkup():
        """
        Function to run check ups for anomaly
        """
        tuple_feed = get_info_from_feed_actions()
        tuple_messages = get_info_from_message()
        current_feed, stat_feed = tuple_feed[0], tuple_feed[1]
        current_messages, stat_messages = tuple_messages[0], tuple_messages[1]

        merged_current = pd.merge(current_feed, current_messages,
                                  on='time_window', how='inner')
        merged_stats = pd.merge(stat_feed, stat_messages,
                                on='time_window', how='inner')

        columns_to_monitor = ['unique_users_feed', 'Likes', 'Views', 'total_messages_sent']

        alread_anomaly = False
        for column in columns_to_monitor:
            alarm, median = check_data_for_anomaly(merged_stats[column].values, merged_current[column].values[0], 5)
            if alarm:
                inform_if_something_is_wrong(merged_stats[column], 
                                            merged_current[column].values[0],
                                            median, column, alread_anomaly)
                alread_anomaly = True     
                
        col_list_partition = ['os', 'gender', 'source', 'country', 'city']
        df_partition = query_for_information_by_partitions()

        for col_name in col_list_partition:
            partition_drop, partition_message = query_analyse_partitions(df_partition,
                                                                            col_name)
            if partition_drop:
                bot = telegram.Bot(token=my_token)
                for chat_id in chat_id_list:
                    bot.sendMessage(chat_id=chat_id, text=partition_message)

    run_checkup()
    
dag_alarm_imosia = dag_alarm_imosia()
