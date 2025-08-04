import pandas as pd
from pandas.plotting import table 
import pandahouse as pandahouse
import io
import requests
from datetime import datetime, timedelta, date
import telegram 
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from PIL import Image
from airflow.decorators import dag, task

# Telegram info
my_token = ''
my_own_chat_id = 
group_chat_id = 
chat_ids_list = [my_own_chat_id, group_chat_id]

# Deafault parametrs for airflow DAGs
default_args = {
        'owner': 'i.mosiagin',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'start_date': datetime(2024, 1, 13),
        }

# Interval for DAG calling - 10:50 AM every day (to gain it around 11)
schedule_interval = '50 10 * * *'

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

def create_file_with_graphs_and_tables(graphs, tables):
    """
    Function that takes list of graphs and dataframes
    and put them in a pdf
    """
    pdf_buffer = io.BytesIO()
    pdf_canvas = canvas.Canvas(pdf_buffer, pagesize=A4)
    page_width, page_height = A4

    # Position for the first graph
    y_position = page_height - 450
    x_position = 25

    counter = 0
    for graph in graphs:
        if y_position < 100 or (counter // 3 == 0 and counter != 0):
                pdf_canvas.showPage()
                y_position = page_height - 450

        image_bytes = graph.read()  # Convert from BytesIO to bytes
        image = Image.open(io.BytesIO(image_bytes))
        if counter != 0:
            pdf_canvas.drawInlineImage(image, x_position,
                                   y_position, width=600, height=400)
        else:
            pdf_canvas.drawInlineImage(image, x_position,
                           y_position, width=450, height=300)

        y_position -= 450
        counter += 1

    x_position = 50
    for table in tables:
        table_data = [list(table.columns)] + table.values.tolist()

        # Draw the table using the Table class
        table_obj = Table(table_data, colWidths=100, rowHeights=20)
        table_obj.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                                       ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                                       ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                                       ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                       ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                                       ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                                       ('GRID', (0, 0), (-1, -1), 1, colors.black)]))
        table_obj.wrapOn(pdf_canvas, 400, 600)

        if (y_position - table_obj._height) < 100 or counter // 3 == 0:
            pdf_canvas.showPage()
            y_position = page_height - 450

        counter += 1
        table_obj.drawOn(pdf_canvas, x_position, y_position)
        y_position -= table_obj._height + 100

    pdf_canvas.save()
    pdf_bytes = pdf_buffer.getvalue()

    return pdf_bytes

@dag(default_args = default_args,
     schedule_interval = schedule_interval,
     catchup = False)
def dag_detailed_information_imosia():
    """
    DAG to collect detailed information on the service
    and send it to Telegram every day at 11 AM
    Send short message, two graphs and a pdf file
    with more detiled information
    """
    
    @task
    def new_users_info():
        """
        Collect information for feed_actions on number of users
        Whose first activity was yesterday
        Return string with this information
        Input: None (ClickHouse DB)
        Output: String with information on new users
        """
        query_new_users = """
            SELECT COUNT(user_id) as new_users_from,
                    source
            FROM (
                SELECT user_id, source
                FROM simulator_20231220.feed_actions
                GROUP BY user_id, source
                HAVING min(toDate(time)) = today()-1
            )
            GROUP BY source
            format TSVWithNames
        """
        df_new_users = ch_get_df(query = query_new_users)
        total_new_users = df_new_users['new_users_from'].sum()
        source_values = {source: df_new_users.loc[df_new_users['source'] == source,
                                'new_users_from'].values[0] for source in df_new_users['source']}

        source_strings = ', '.join(f"{source}: {value}" for source, value in source_values.items())
        new_users_message = f"{total_new_users} new users in total:\n{source_strings}."

        return new_users_message

    @task
    def query_for_dateiled_information():
        """
        Function takes information from ClickHouse DB
        Containing information on actions and metrics
        over 15 minutes intervals in the last 8 days
        Input: None
        Output: pandas dataframe with aggreageted information
        """
        query_detailed_graphs = """
            SELECT 
                toStartOfHour(toDateTime(time)) 
                + toIntervalSecond(floor(toUInt16(toMinute(toDateTime(time))) / 15) * 15 * 60)
                AS time_window,
                count(DISTINCT user_id) AS unique_users,
                countIf(action = 'like') AS Likes,
                countIf(action = 'view') AS Views,
                count(*)/uniq(user_id) as action_per_active_user,
                count(action) AS Total_actions,
                ROUND(Likes / Views, 3) AS CTR
            FROM simulator_20231220.feed_actions
            WHERE toDateTime(time) > now() - 8 * 24 * 60 * 60
            GROUP BY time_window
            ORDER BY time_window DESC
            FORMAT TSVWithNames
        """
        # Dataframe modifications
        df = ch_get_df(query = query_detailed_graphs)
        df['time_window'] = pd.to_datetime(df['time_window'])
        df = df.sort_values(by='time_window')
        shift_time = pd.to_datetime('now') - pd.DateOffset(minutes=15)

        # Separating data
        last_24_hours = df[(df['time_window'] >= df['time_window'].max() - pd.DateOffset(days=1, minutes=15))
                          & (df['time_window'] < df['time_window'].max() - pd.DateOffset(minutes=15))]
        past_24_to_48_hours = df[(df['time_window'] >= df['time_window'].max() - pd.DateOffset(days=2, minutes=15))
                                 & (df['time_window'] < df['time_window'].max() - pd.DateOffset(days=1, minutes=15))]
        past_7_days = df[(df['time_window'] >= df['time_window'].max() - pd.DateOffset(weeks=1, minutes=15)) 
                         & (df['time_window'] < df['time_window'].max() - pd.DateOffset(days=6, minutes=15))]
        past_24_to_48_hours.loc[:, 'time_window'] += pd.DateOffset(days=1)
        past_7_days.loc[:, 'time_window'] += pd.DateOffset(days=6)
        df['time_window'] = df['time_window'].dt.strftime('%H:%M')

        # Plotting data for each columns
        colors_list = ['peru', 'darkseagreen', 'goldenrod', 'plum', 'darkkhaki', 'lightcoral']
        columns_to_plot = df.columns.difference(['time_window'])
        graph_io_dict = {}
        for column, color in zip(columns_to_plot, colors_list):
            plt.figure(figsize=(10, 6))
            plt.plot(last_24_hours['time_window'], last_24_hours[column], label='Last 24 hours', linewidth=2, color=color)
            plt.plot(past_24_to_48_hours['time_window'], past_24_to_48_hours[column], label='24 to 48 hours ago', linestyle='--', linewidth=2, color=color)
            plt.plot(past_7_days['time_window'], past_7_days[column], label='7 days ago', linestyle=':', linewidth=2, color=color)

            plt.xlabel('Last 24h (time)')
            plt.ylabel(column)
            plt.title(f'{column} Over Time')
            plt.legend()
            date_format = '%H:%M'
            plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter(date_format))

            graph_io = io.BytesIO()
            plt.savefig(graph_io)
            graph_io.seek(0)
            graph_io.name = f'{date.today()}_{column}.png'
            graph_io_dict[column] = graph_io
            plt.close()

        return graph_io_dict    
        
    @task
    def information_on_message_actions():
        """
        Takes information on message actions from
        ClickHouse DB, extract statistics and 
        saves resulting talbe in png format
        Input: None
        Output: png table, and message with info
        """

        query_message_actions = """
                        SELECT event_date,
                            count(distinct user_id) as unique_users,
                            SUM(messages_sent) as total_messages_sent,
                            ROUND(SUM(messages_sent) / count(distinct user_id), 2) as messages_per_user,
                            ROUND(SUM(users_sent) / count(distinct user_id), 2) as interactors_per_user
                        FROM (
                            SELECT 
                                user_id, toDate(time) as event_date,
                                count (receiver_id) as messages_sent,
                                count (distinct receiver_id) as users_sent
                            FROM simulator_20231220.message_actions
                            GROUP BY user_id, toDate(time)
                        ) 
                        GROUP BY event_date
                        ORDER BY event_date DESC
                        format TSVWithNames
        """

        df = ch_get_df(query = query_message_actions)

        # Interesting dates
        df['event_date'] = pd.to_datetime(df['event_date'])
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        one_week_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

        # Filter the DataFrame for the specified dates
        yesterday_data = df[df['event_date'] == yesterday]
        two_days_ago_data = df[df['event_date'] == two_days_ago]
        one_week_ago_data = df[df['event_date'] == one_week_ago]

        # Statistic over the whole period
        entire_period_data = df.agg({
            'unique_users': 'mean',
            'total_messages_sent': 'mean',
            'messages_per_user': 'mean',
            'interactors_per_user': 'mean'
        }, axis=0)
        entire_period_data = pd.DataFrame(entire_period_data).T.round(2)
        entire_period_data['event_date'] = None

        # Combining results
        result_df = pd.concat([yesterday_data, two_days_ago_data, one_week_ago_data, entire_period_data])
        index_values = ['Yesterday', 'Two Days Ago', 'One Week Ago', 'Entire Period (AVG)']
        result_df['Period'] = index_values

        result_df['event_date'] = result_df['event_date'].dt.strftime('%Y-%m-%d')
        column_mapping = {
            'event_date': 'Date',
            'unique_users': 'DAU (messages)',
            'total_messages_sent': 'Messages sent',
            'messages_per_user': 'Messages per user',
            'interactors_per_user': 'Pers per user'
        }
        result_df = result_df.rename(columns=column_mapping)
        result_df = result_df[['Period', 'Date', 'DAU (messages)', 'Messages sent',
                                           'Messages per user', 'Pers per user']]

        #Prepare nice picture
        fig, ax = plt.subplots()
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)
        ax.set_frame_on(False)

        table = plt.table(cellText=result_df.values,
                      colLabels=result_df.columns,
                      cellLoc='center',
                      loc='center',
                      colColours=['#f0f0f0']*len(result_df.columns)
        )
        table.auto_set_font_size(True)
        table.set_fontsize(10)
        table.scale(3, 3)

        table_with_messages = io.BytesIO()
        plt.savefig(table_with_messages, bbox_inches='tight', pad_inches=0.05)
        table_with_messages.seek(0)
        table_with_messages.name = f'{date.today()}_messages.png'
        plt.close()


        result_df = result_df[['Period', 'DAU (messages)', 'Messages sent',
                                           'Messages per user', 'Pers per user']]

        return result_df
    
    @task
    def information_on_our_users():
        """
        Function takes information from
        ClickHouse DB, analyse by
        partiotion on users devices,
        geographical and presonal data
        and provide pivot pandas df
        Input: None
        Output: List of pandas df
        """
        query_users_info = """
            WITH age_intervals AS (
                SELECT
                    user_id,
                    gender,
                    os,
                    country,
                    city,
                    source,
                    time,
                    CASE
                      WHEN age <= 18 THEN '0-18'
                      WHEN age <= 25 THEN '19-25'
                      WHEN age <= 35 THEN '26-35'
                      WHEN age <= 50 THEN '36-50'
                      ELSE '51+'
                    END AS age_interval
                FROM simulator_20231220.feed_actions
            )

            SELECT
              gender,
              os,
              country,
              city,
              source,
              age_interval,
              COUNT(DISTINCT user_id) AS users_count,
              COUNT(DISTINCT user_id) / MAX(total_users) AS users_ratio
            FROM age_intervals
            CROSS JOIN (
              SELECT COUNT(DISTINCT user_id) AS total_users
              FROM age_intervals
            ) t
            GROUP BY gender, os, country, city, source, age_interval
            format TSVWithNames
        """

        def get_aggregated_query_for_specific_day(time_delta):
            query_users_info_yesterday = f"""
                WITH age_intervals AS (
                    SELECT
                        user_id,
                        gender,
                        os,
                        country,
                        city,
                        source,
                        time,
                        CASE
                          WHEN age <= 18 THEN '0-18'
                          WHEN age <= 25 THEN '19-25'
                          WHEN age <= 35 THEN '26-35'
                          WHEN age <= 50 THEN '36-50'
                          ELSE '51+'
                        END AS age_interval
                  FROM simulator_20231220.feed_actions
                )

                SELECT
                      gender,
                      os,
                      country,
                      city,
                      source,
                      age_interval,
                      COUNT(DISTINCT user_id) AS users_count,
                      COUNT(DISTINCT user_id) / MAX(total_users) AS users_ratio
                FROM (
                    SELECT
                        user_id,
                        gender,
                        os,
                        country,
                        city,
                        source,
                        age,
                        CASE
                          WHEN age <= 18 THEN '0-18'
                          WHEN age <= 25 THEN '19-25'
                          WHEN age <= 35 THEN '26-35'
                          WHEN age <= 50 THEN '36-50'
                          ELSE '51+'
                        END AS age_interval
                    FROM simulator_20231220.feed_actions
                    WHERE toDate(time) = yesterday() {time_delta}
                ) t
                CROSS JOIN (
                  SELECT COUNT(DISTINCT user_id) AS total_users
                  FROM age_intervals
                ) t2
                GROUP BY gender, os, country, city, source, age_interval
                format TSVWithNames
            """
            df =  ch_get_df(query = query_users_info_yesterday)
            return df

        df_full = ch_get_df(query = query_users_info)
        df_yesterday = get_aggregated_query_for_specific_day('')
        df_two_days_ago = get_aggregated_query_for_specific_day('- 1')
        df_one_week_ago = get_aggregated_query_for_specific_day('- 7')

        def calculate_user_ratio(df, col_name):
            total = df['users_count'].sum()
            ratio_df = df.groupby(col_name)['users_count'].sum().reset_index()
            ratio_df['users_ratio'] = ratio_df['users_count'] / total
            ratio_df['users_ratio'] = ratio_df['users_ratio'].round(3)
            return ratio_df

        def merge_and_sum(yesterday, two_days_ago, one_week_ago, all_time, col_name):
            # Calculate user ratios for each time period
            yesterday_ratio = calculate_user_ratio(yesterday, col_name)
            two_days_ago_ratio = calculate_user_ratio(two_days_ago, col_name)
            one_week_ago_ratio = calculate_user_ratio(one_week_ago, col_name)
            all_time_ratio = calculate_user_ratio(all_time, col_name)

            # Merge the four dataframes based on col_name
            merged_df = pd.merge(yesterday_ratio, two_days_ago_ratio, on=col_name, how='outer', suffixes=('_yesterday', '_two_days_ago'))
            merged_df = pd.merge(merged_df, one_week_ago_ratio, on=col_name, how='outer', suffixes=('', '_week_ago'))
            merged_df = pd.merge(merged_df, all_time_ratio, on=col_name, how='outer', suffixes=('', '_overall'))

            merged_df = merged_df.fillna(0)
            count_columns = [col for col in merged_df.columns if 'count' in col and 'country' not in col]
            merged_df = merged_df.drop(columns=count_columns)
            merged_df.columns = [col.replace('users_ratio_', '') for col in merged_df.columns]
            merged_df.rename(columns={'users_ratio': 'week_ago'}, inplace=True)


            merged_df = merged_df.sort_values(by='yesterday', ascending=False)
            merged_df = merged_df.head(10)
            merged_df = merged_df.reset_index(drop=True)

            return merged_df



        col_list = ['os', 'country', 'source', 'age_interval', 'city', 'gender']
        list_of_df = []

        for col_name in col_list:
            merged_df = merge_and_sum(df_yesterday, df_two_days_ago, df_one_week_ago, df_full, col_name)
            list_of_df.append(merged_df)

        return list_of_df

    @task
    def weelky_information_on_users_and_actions():
        """
        Function downloads information on active users
        and number of interaction with feed aggregated
        by weeks and provide corresonding graphs
        Input: None (upload from ClickHouse)
        Output: png image with WAU and Weekly Actions graphs

        """
        query_WAU = """
                SELECT toMonday(toDateTime(time)) AS date,
                          count(DISTINCT user_id) AS WAU,
                          count(*) AS Weekly_actions
               FROM simulator_20231220.feed_actions
                   GROUP BY toMonday(toDateTime(time))
                   ORDER BY 1 desc
                format TSVWithNames
        """

        df = ch_get_df(query = query_WAU)
        df['date'] = pd.to_datetime(df['date'])

        fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        # WAU
        ax[0].scatter(df['date'], df['WAU'], color='blue', label='WAU')
        ax[0].set_ylabel('WAU')
        ax[0].set_title(' WAU')

        # Weekly_actions
        ax[1].scatter(df['date'], df['Weekly_actions'], color='green',
                      label='Weekly Actions')
        ax[1].set_ylabel('Weekly Feed Actions')
        ax[1].set_title('Weekly Feed Actions')

        ax[0].legend()
        ax[1].legend()
        ax[1].xaxis.set_major_locator(mdates.WeekdayLocator())
        ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        plt.setp(ax[1].xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()

        WAU_graph = io.BytesIO()
        plt.savefig(WAU_graph)
        WAU_graph.seek(0)
        WAU_graph.name = f'{date.today()}_WAU.png'
        plt.close()

        return WAU_graph

    @task
    def retained_new_gone_graph():
        """
        Function to take data from ClickHouse
        and then proceed it to make 
        new-retained-gone graph on users
        with partition by weeks
        Input: None (upload from ClickHouse)
        Output: png - graph
        """
        query_RGN = """
            SELECT toStartOfDay(toDateTime(this_week)) AS date,
                   status AS status,
                   AVG(delta_users) AS delta_users
            FROM
                    (SELECT this_week,
                              previous_week,
                              - uniq(user_id) AS delta_users,
                              status
                    FROM
                        (SELECT user_id,
                             groupUniqArray(toMonday(toDate(time))) AS weeks_visited,
                             addWeeks(arrayJoin(weeks_visited), + 1) this_week,
                             if(has(weeks_visited, this_week) = 1, 'retained', 'gone') AS status,
                             addWeeks(this_week, -1) AS previous_week
                          FROM simulator_20231220.feed_actions
                          GROUP BY user_id
                        )
                WHERE status = 'gone'
                GROUP BY this_week,
                        previous_week,
                        status
                HAVING this_week != addWeeks(toMonday(today()), + 1)

                UNION ALL

                SELECT this_week,
                        previous_week,
                        toInt64(uniq(user_id)) AS delta_users,
                        status
                   FROM
                         (SELECT user_id,
                                 groupUniqArray(toMonday(toDate(time))) AS weeks_visited,
                                 arrayJoin(weeks_visited) this_week,
                                 if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') AS status,
                                 addWeeks(this_week, -1) AS previous_week
                          FROM simulator_20231220.feed_actions
                          GROUP BY user_id)
                GROUP BY this_week,
                            previous_week,
                            status) AS virtual_table
            GROUP BY status,
                     toStartOfDay(toDateTime(this_week))
            ORDER BY date ASC
            format TSVWithNames
        """

        df = ch_get_df(query = query_RGN)
        df['date'] = pd.to_datetime(df['date']).dt.date
        update_delta_users = lambda x:\
                             x['delta_users'] + df[(df['status'] == 'retained')\
                            & (df['date'] == x['date'])]['delta_users'].sum()\
                             if x['status'] == 'new' else x['delta_users']

        df['delta_users'] = df.apply(update_delta_users, axis=1)


        plt.figure(figsize=(10, 6))
        sns.barplot(x='date', y='delta_users', hue='status',
                    data=df,
                    palette={'retained': 'b', 'new': 'g', 'gone': 'r'},
                    dodge = False)
        plt.xlabel('Date')
        plt.ylabel('Delta Users')
        plt.title('Retained, New, Gone Chart')
        plt.xticks(rotation=45, ha='right')
        plt.legend(title='Status')

        RGN_graph = io.BytesIO()
        plt.savefig(RGN_graph)
        RGN_graph.seek(0)
        RGN_graph.name = f'{date.today()}_RGN.png'
        plt.close()

        return RGN_graph
    
    @task
    def retention_information_and_graphs(type_of_graph):
        """
        Function upload infromation from ClickHouse DB
        Calculate retention for users came organic
        and trhought advertisement
        and create two retention graphs for each category
        Input: None
        Output: 2 png with retention graphs
        """
        query_retention = """
        SELECT day AS date,
                      start_date AS start_date,
                      AVG(abs_users) AS still_there_abs,
                      AVG(organic_users) AS still_there_organic
        FROM
          (with abs_users AS
             (SELECT toString(start_day) start_day,
                     toString(day) day,
                                   count(user_id) AS abs_users
              FROM
                (SELECT *
                 FROM
                   (SELECT user_id,
                           min(toDate(time)) AS start_day
                    FROM simulator_20231220.feed_actions
                    GROUP BY user_id
                    HAVING source = 'ads') t1
                 JOIN
                   (SELECT DISTINCT user_id,
                                    toDate(time) AS day
                    FROM simulator_20231220.feed_actions) t2 USING user_id
                 WHERE start_day >= today() - 20 )
              GROUP BY start_day,
                       day) ,
                organic_users AS
             (SELECT toString(start_day) start_day,
                     toString(day) day,
                                   count(user_id) AS organic_users
              FROM
                (SELECT *
                 FROM
                   (SELECT user_id,
                           min(toDate(time)) AS start_day
                    FROM simulator_20231220.feed_actions
                    GROUP BY user_id
                    HAVING source = 'organic') t1
                 JOIN
                   (SELECT DISTINCT user_id,
                                    toDate(time) AS day
                    FROM simulator_20231220.feed_actions) t2 USING user_id
                 WHERE start_day >= today() - 20 )
              GROUP BY start_day,
                       day) SELECT abs_users.start_day as start_date,
                                   abs_users.day as day,
                                   organic_users,
                                   abs_users
           FROM abs_users
           JOIN organic_users ON abs_users.start_day = organic_users.start_day
           AND abs_users.day = organic_users.day) AS virtual_table
        GROUP BY day,
                 start_date
        format TSVWithNames"""
        df = ch_get_df(query = query_retention)

        def plot_cohort_ltv_normalized(df, col_name, name):
            """
            Plot normalized Cohort LTV graph based on the given DataFrame.

            Parameters:
            - df: Pandas DataFrame with columns 'start_date', 'date', and 'still_there'.
            """

            df = df.sort_values(by=['start_date', 'date'])
            df['normalization_factor'] = df.groupby('start_date')[col_name].transform('first')
            df['normalized_cumulative_users'] = df[col_name] / df['normalization_factor']
            cohort_ltv = df.pivot_table(index='start_date', columns='date', values='normalized_cumulative_users', aggfunc='last')

            # Cohort LTV graph
            plt.figure(figsize=(12, 8))
            plt.imshow(cohort_ltv.values, cmap='Blues', interpolation='nearest')
            plt.colorbar(label='Normalized Cumulative Users')

            plt.xticks(np.arange(len(cohort_ltv.columns)), cohort_ltv.columns, rotation=45)
            plt.yticks(np.arange(len(cohort_ltv.index)), cohort_ltv.index)
            plt.xlabel('Date')
            plt.ylabel('Start Date')
            plt.title(f'Normalized Cohort LTV Graph for {name}')

            # save as png
            graph = io.BytesIO()
            plt.savefig(graph)
            graph.seek(0)
            graph.name = f'{date.today()}_RGN.png'
            plt.close()
            return graph


        if type_of_graph == 'abs':
            retention_graph = plot_cohort_ltv_normalized(df, 'still_there_abs', 'abs')
        else:
            retention_graph = plot_cohort_ltv_normalized(df, 'still_there_organic', 'organic')

        return retention_graph

    @task
    def combine_message(table_df_with_messages, new_users_message):
        '''
        Function that combines all the messages
        To provide final string to send
        '''

        message_about_messages = f"{table_df_with_messages['DAU (messages)'].iloc[0]} people used messages\n" \
                                  f"and sent {table_df_with_messages['Messages sent'].iloc[0]} messages in total"

        message = 'Detailed information on yesterday\n'
        message += new_users_message
        message += '\n'
        message += message_about_messages
        message += '\n'

        return message

    @task
    def make_file(WAU_graph, RGN_graph, table_with_messages, abs_retention_graph, organic_retention_graph, list_of_df_with_users_info, graph_io_dict):
        """
        Function to prepare pdf with detailed
        information that should be sent
        """
        total_actions_graph = graph_io_dict['Total_actions']
        unique_users_graph = graph_io_dict['unique_users']
        list_of_graphs_for_file = []
        list_of_graphs_for_file.append(WAU_graph)
        list_of_graphs_for_file.append(RGN_graph)
        names_needed_from_io = ['CTR', 'Likes', 'Views', 'action_per_active_user']
        for name in names_needed_from_io:
            list_of_graphs_for_file.append(graph_io_dict[name])
        list_of_graphs_for_file.append(abs_retention_graph)
        list_of_graphs_for_file.append(organic_retention_graph)
        #list_of_graphs_for_file.append(table_with_messages)
        list_of_df_with_users_info.append(table_with_messages)


        file_content = create_file_with_graphs_and_tables(list_of_graphs_for_file, list_of_df_with_users_info)

        return total_actions_graph, unique_users_graph, file_content

    
    
    @task
    def sent_information(message, total_actions_graph, unique_users_graph, file_object, chat_ids_list):
        """
        Function takes message and graph to send to Telegram,
        as well as chat_id and sends them
        Input: message - string
        total_actions_graph, unique_users_graph - png
        file_object- file object
        chat_id_list - list of intgers integer
        """
        for chat_id in chat_ids_list:
            bot = telegram.Bot(token=my_token)
            bot.sendMessage(chat_id=chat_id, text=message)
            total_actions_graph.seek(0)
            bot.sendPhoto(chat_id=chat_id, photo=total_actions_graph)
            unique_users_graph.seek(0)
            bot.sendPhoto(chat_id=chat_id, photo=unique_users_graph)
            today = datetime.today().strftime('%Y-%m-%d')
            filename_with_date = f"Detailed_analysis_{today}.pdf"
            bot.send_document(chat_id=chat_id, document=io.BytesIO( file_object), filename=filename_with_date)
    
    new_users_message = new_users_info()
    graph_io_dict = query_for_dateiled_information()
    table_df_with_messages = information_on_message_actions()
    WAU_graph = weelky_information_on_users_and_actions()
    RGN_graph = retained_new_gone_graph()  
    list_of_df_with_users_info = information_on_our_users()
    abs_retention_graph= retention_information_and_graphs(type_of_graph='abs')
    organic_retention_graph= retention_information_and_graphs(type_of_graph='organic')
    final_message = combine_message(table_df_with_messages, new_users_message)
    total_actions_graph, unique_users_graph, file_object = make_file(WAU_graph, RGN_graph, table_df_with_messages,
                                                                      abs_retention_graph, organic_retention_graph, list_of_df_with_users_info, graph_io_dict)        
    sent_information(final_message, total_actions_graph, unique_users_graph, file_object, chat_ids_list)        
    
    
dag_detailed_information_imosia = dag_detailed_information_imosia()


