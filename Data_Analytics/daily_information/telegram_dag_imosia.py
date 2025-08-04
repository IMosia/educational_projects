import pandas as pd
import pandahouse as pandahouse
import io
import requests
from datetime import datetime, timedelta, date
import telegram 
import matplotlib.pyplot as plt
import seaborn as sns
from airflow.decorators import dag, task

# Telegram info
my_token = ''
my_own_chat_id = 
group_chat_id = 

# Deafault parametrs for airflow DAGs
default_args = {
        'owner': 'i.mosiagin',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'start_date': datetime(2024, 1, 13),
        }

# Interval for DAG calling - 11 AM every day
schedule_interval = '0 11 * * *'

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

@dag(default_args = default_args,
     schedule_interval = schedule_interval,
     catchup = False)
def dag_telegram_notification_imosia():
    """
    DAG for daily analytics with Telegram
    1. Download info from table feed_actions for the last 7 days
        Info: date, DAU, likes, views, CTR
    2. Prepares message string with information on yesterday
    3. Prepares graphs with information on the last 7 days
    4. Sent message and graphs to Telegram (personal and group chat)
    """
    
    @task
    def data_from_database():
        """
        Download data from feed_actions
        date, DAU, views, likes, CTR for the last 7 days
        Input: None
        Output: pandas df
        """
        query_feed_actions = """
            SELECT 
                toDate(time) as date,
                count(DISTINCT user_id) AS DAU,
                countIf(action='like') AS Likes,
                countIf(action='view') AS Views,
                ROUND(Likes/Views, 3) AS CTR
            FROM simulator_20231220.feed_actions
            WHERE toDate(time) > today() - 8 
                    AND toDate(time) < today()
            GROUP BY date
            ORDER BY date DESC
            LIMIT 7
            format TSVWithNames
        """

        df = ch_get_df(query = query_feed_actions)
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    @task
    def collect_information_on_the_previous_day(df):
        """
        Takes dataframe with information on the last 7 days, selects data for yestarday
        Then checks that it is not empty, and provides massage with information
        Input: pandas dataframe
        Output: message - string
        """
        yesterday = date.today() - timedelta(days = 1)
        data = df[df['date'] == yesterday]

        if data.empty or data['DAU'][0] <= 0:
            message = 'There is no data for yesterday, something is wrong!!!'
        else:
            message = 'Daily metrics information\n'
            for index, row in data.iterrows():
                for column in data.columns:
                    message += (f"{column}: {row[column]}\n")
            message += '\n'
        return message

    @task
    def preapare_graph_with_seven_days_stats(df):
        """
        Takes information on the last 7 days and prepare
        4 graphs on 4 picutre containg information
        on likes, views, DAU, and CTR
        saves it as png
        Input: df - pandas dataframe
        Output: graph - png object
        """
        fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(10, 16), sharex=True)
        colors = ['lightblue', 'tomato', 'palegreen', 'mediumorchid']
        df = df.sort_values(by='date')

        columns = ['DAU', 'Likes', 'Views', 'CTR']
        for i, (col, color) in enumerate(zip(columns, colors)):
            sns.stripplot(x='date', y=col, data=df, ax=axes[i], jitter=True, c
                          olor=color, size=16, alpha=0.7)
            axes[i].scatter(df['date'].iloc[-1].strftime('%Y-%m-%d'),
                            df[col].iloc[-1], color='red', s=200, zorder=5, alpha=0.7)
            axes[i].axhline(y=df[col].iloc[-1],
                            color='red', linestyle='--', linewidth=1)
            
            axes[i].set_ylabel(col, fontsize=16)
            axes[i].set_title(f'{col} during the last week', fontsize=16)
            axes[i].tick_params(axis='both', which='major', labelsize=14)
            axes[i].grid(alpha=0.2)

        axes[-1].set_xlabel('Date', fontsize=14)
        axes[-1].tick_params(axis='both', which='major', labelsize=14)
        axes[-1].grid(alpha=0.2)
        plt.tight_layout()

        graph = io.BytesIO()
        plt.savefig(graph)
        graph.seek(0)
        graph.name = f'{date.today()}_metrics.png'
        plt.close()

        return graph

    @task
    def sent_information(message, graph, chat_id):
        """
        Function takes message and graph to send to Telegram,
        as well as chat_id and sends them
        Input: message - string
        Graph - png object
        chat_id - integer
        """
        bot = telegram.Bot(token=my_token)
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=graph)
        return None
      
    df = data_from_database()
    message = collect_information_on_the_previous_day(df)
    graph = preapare_graph_with_seven_days_stats(df)
    sent_information(message, graph, my_own_chat_id)
    sent_information(message, graph, group_chat_id)
    
dag_telegram_notification_imosia = dag_telegram_notification_imosia()