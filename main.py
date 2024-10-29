"""
    This script fetches public data from 'https://www.epexspot.com/en/market-data'
    for european electricity market for both continous and auction trading modalities
    for day-ahead and intraday sub-modalities and for several consequent days.
    The latter is done to assure that the data had had time to be updated and finalized.
    NOTE when collating the data for several files use the oldest data thus.

    For simplicity GB data is excluded

    Inspiration: https://github.com/elgohr/EPEX-DE-History
"""

from datetime import datetime,timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import warnings
import os
warnings.simplefilter(action='ignore', category=FutureWarning)

def get_time_axis_hour(date_str:str,start_hour:int) -> pd.Series :
    index_hours = [
        "00 - 01", "01 - 02", "02 - 03", "03 - 04", "04 - 05", "05 - 06",
        "06 - 07", "07 - 08", "08 - 09", "09 - 10", "10 - 11", "11 - 12",
        "12 - 13", "13 - 14", "14 - 15", "15 - 16", "16 - 17", "17 - 18",
        "18 - 19", "19 - 20", "20 - 21", "21 - 22", "22 - 23", "23 - 24"
    ]
    if start_hour == 12:
        index_hours = index_hours[int(len(index_hours)/2):]

    # Convert to Pandas Series
    series_hours = pd.Series(index_hours)

    # Function to parse the hour and convert to timestamp
    def parse_hour_to_timestamp(hour_str):
        hour = int(hour_str.split(' - ')[0])  # Split the string and convert the first part to an integer
        timestamp = pd.to_datetime(f"{date_str} {hour}:00:00")  # Create a timestamp string and convert to datetime
        return timestamp

    return series_hours.apply(parse_hour_to_timestamp)

def fetch_spot_data(date_str:str, url:str) -> pd.DataFrame:
    # Fetch the webpage
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Fetch the webpage
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an HTTPError for bad responses

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table - adjust the selector as needed based on the actual table structure
    table = soup.find('table')

    # Convert the table to a DataFrame, if a table is found
    data_frame = pd.read_html(str(table))[0] if table else None
    if data_frame is None:
        print(f"DATA NOT FOUND FOR: {url}")
        return pd.DataFrame()

    # print(f"Data size {len(data_frame)}")

    # Display the DataFrame
    data_frame = pd.DataFrame(data_frame)

    if len(data_frame) == 168:
        # get hourly data (skip 15 min intervals)
        data_frame_lim = data_frame.iloc[::7]
    elif len(data_frame) == 72:
        # get hourly data (skip 30 min intervals)
        data_frame_lim = data_frame.iloc[::3]
    elif len(data_frame) == 74:
        # get hourly data (skip 30 min intervals)
        print(f'WARNING data length ({len(data_frame)}) suggests Long Clock Change Day with 25 hours. Collection might be erroneous')
        data_frame_lim = data_frame.iloc[::3]
        data_frame_lim = data_frame_lim.iloc[:24]  # Adjust to include 25 hours for the day
    elif len(data_frame) == 24:
        # get hourly data (skip 1 hour intervals)
        data_frame_lim = data_frame
        pass
    elif len(data_frame) == 172 or len(data_frame) == 174:
        # Special handling for "Long Clock Change Day" (skip 15 min intervals but account for extra hour)
        print(f'WARNING data length ({len(data_frame)}) suggests Long Clock Change Day with 25 hours. Collection might be erroneous')
        data_frame_lim = data_frame.iloc[::7]
        # data_frame_lim = data_frame_lim.iloc[:25]  # Adjust to include 25 hours for the day
        data_frame_lim = data_frame_lim.iloc[:24]  # Adjust to include 25 hours for the day
    else:
        raise ValueError(f"Data size {len(data_frame)} not supported (see 168 for 15min and 72 for 30min)")

    # Function to parse the hour and convert to timestamp add timestamp column  and Convert to Pandas Series
    data_frame_lim.index = get_time_axis_hour(date_str, 0)

    #.reset_index(inplace=True)
    data_frame_lim = data_frame_lim.reset_index().rename(columns={'index': 'date'})
    #data_frame_lim.reset_index(inplace=True)

    # drop columns that are 100% nans
    data_frame_lim.dropna(axis='columns', how='all', inplace=True)

    # map the column names for easy processing
    name_map = {
        'Low (€/MWh)': 'low',
        'High (€/MWh)': 'high',
        'Last (€/MWh)': 'last',
        'Weight Avg. (€/MWh)': 'weighted_avg',
        'ID Full (€/MWh)': 'id_full',
        'ID1 (€/MWh)': 'id1',
        'ID3 (€/MWh)': 'id3',
        'Buy Volume (MWh)': 'buy_volume',
        'Sell Volume (MWh)': 'sell_volume',
        'Volume (MWh)': 'total_volume',

        # -----------------------------


    }
    data_frame_lim = data_frame_lim.rename(columns=name_map)

    return data_frame_lim

def get_time_axis_15min(date_str:str):
    # Add timestamp column with 15-minute intervals
    index_15min_intervals = [
        f"{hour:02}:{minute:02}" for hour in range(24) for minute in range(0, 60, 15)
    ]

    # Convert to Pandas Series
    series_15min = pd.Series(index_15min_intervals)

    # Function to parse the time string and convert to timestamp
    def parse_time_to_timestamp(time_str):
        return pd.to_datetime(f"{date_str} {time_str}:00")

    # Apply the function to each item in the series to create datetime objects
    timestamps = series_15min.apply(parse_time_to_timestamp)

    return timestamps

def get_time_axis_30min(date_str:str):
    # Define the intervals for each 30 minutes
    index_intervals = [
                          f"{hour:02d}:00" for hour in range(24)
                      ] + [
                          f"{hour:02d}:30" for hour in range(24)
                      ]

    # Sort intervals since they are created in two halves (00:00, 01:00..., 23:00, 00:30, 01:30..., 23:30)
    index_intervals = sorted(index_intervals, key=lambda x: (int(x.split(':')[0]), int(x.split(':')[1])))

    # Convert to Pandas Series
    series_intervals = pd.Series(index_intervals)

    # Function to convert the hour-minute string to timestamp
    def parse_interval_to_timestamp(time_str):
        timestamp = pd.to_datetime(f"{date_str} {time_str}:00")  # Create a timestamp from date and time
        return timestamp

    # Apply the function to each item in the series to create timestamps
    timestamps = series_intervals.apply(parse_interval_to_timestamp)
    return timestamps

def fetch_auction_data(delivery_date_str:str, url:str) -> pd.DataFrame:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Fetch the webpage
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an HTTPError for bad responses

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Initialize a list to hold all rows of data
    data = []

    # Find all rows in the table within <tr class="child"> and <tr class="child impair">
    rows = soup.find_all('tr', class_=["child", "child impair"])

    # Iterate through each row and fetch columns
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)  # Add the column data to the list of rows

    # Create a DataFrame using the extracted data
    data_frame = pd.DataFrame(data, columns=['Buy Volume (MWh)', 'Sell Volume (MWh)', 'Volume (MWh)', 'Price (€/MWh)'])

    for col in data_frame.columns:
        data_frame[col] = data_frame[col].str.replace(',', '').astype(float)

    # Display the DataFrame
    if len(data_frame) == 0:
        print(f"DATA NOT FOUND FOR: {url}")
        return pd.DataFrame()
        # elif len(data_frame) == 168:
    elif len(data_frame) == 12:
        # get hourly data (skip 1 hour intervals)
        print(f"Data size {len(data_frame)} (assuming hourly interval starting at 12.00")
        data_frame_lim = data_frame
        data_frame_lim.index = get_time_axis_hour(delivery_date_str, start_hour=12)
    elif len(data_frame) == 24:
        print(f"Data size {len(data_frame)} (assuming hourly interval starting at 00.00")
        # get hourly data (skip 1 hour intervals)
        data_frame_lim = data_frame
        data_frame_lim.index = get_time_axis_hour(delivery_date_str, start_hour=0)
    elif len(data_frame) == 48:
        print(f"Data size {len(data_frame)} (assuming half-hourly interval starting at 12.00")
        # get hourly data (skip 30 min intervals)
        data_frame_lim = data_frame
        data_frame_lim.index = get_time_axis_30min(delivery_date_str)
    elif len(data_frame) == 50:
        print(f"WARNING Data size {len(data_frame)} (assuming half-hourly interval starting at 12.00 with extra hour")
        # get hourly data (skip 30 min intervals)
        data_frame_lim = data_frame[:48]
        data_frame_lim.index = get_time_axis_30min(delivery_date_str)
    elif len(data_frame) == 96:
        print(f"Data size {len(data_frame)} (assuming quarter-hourly interval starting at 12.00")
        # get hourly data (skip 15 min intervals)
        data_frame_lim = data_frame
        data_frame_lim.index = get_time_axis_15min(delivery_date_str)
    elif len(data_frame) == 100:
        print(f"WARNING Data size {len(data_frame)} (assuming quarter-hourly interval starting at 12.00 but with extra hour")
        data_frame_lim = data_frame[:96]
        data_frame_lim.index = get_time_axis_15min(delivery_date_str)
    else:
        raise ValueError(f"Data size {len(data_frame)} not supported (see 96 for 15min and 24 for 1hour)")

    # Apply the function to each item in the series
    # data_frame_lim = series_hours.apply(parse_hour_to_timestamp)

    # Display the resulting timestamps
    #.reset_index(inplace=True)
    data_frame_lim = data_frame_lim.reset_index().rename(columns={'index': 'date'})
    #data_frame_lim.reset_index(inplace=True)

    # drop columns that are 100% nans
    data_frame_lim.dropna(axis='columns', how='all', inplace=True)

    # map the column names for easy processing
    name_map = {
        'Buy Volume (MWh)': 'Buy_Volume',
        'Sell Volume (MWh)' : 'Sell_Volume',
        'Volume (MWh)':'Volume',
        'Price (€/MWh)':'Price',
        # -----------------------------
    }
    data_frame_lim = data_frame_lim.rename(columns=name_map)

    return data_frame_lim



def collect_continuous_market_data(start_date, end_date):
    market_type = 'continuous'
    for market_area in [
            'AT','BE','CH','DE','DK1','DK2','FI','FR','NL',
            'NO1','NO2','NO3','NO4','NO5','PL','SE1','SE2','SE3','SE4'
        ]:
        df = pd.DataFrame()

        for date in pd.date_range(start=start_date, end=end_date):
            date_str = date.strftime("%Y-%m-%d")
            print(f"{market_type} | {market_area} | {date_str}")

            url = f"https://www.epexspot.com/en/market-data?market_area={market_area}&auction=&trading_date=&delivery_date={date_str}&underlying_year=&modality=Continuous&sub_modality=&technology=&data_mode=table&period=&production_period=&product=60"

            df_i = fetch_spot_data(date_str,  url=url)
            if not df_i is None:
                df = pd.concat([df,df_i])

        df_sorted = df.sort_values(by='date')

        if not os.path.isdir(f"./data/{market_area}"):
            os.mkdir(f"./data/{market_area}")
        if not os.path.isdir(f"./data/{market_area}/{market_type}"):
            os.mkdir(f"./data/{market_area}/{market_type}")

        temp_index = pd.DatetimeIndex(df['date'])
        frequency = temp_index.inferred_freq

        fname = f'./data/{market_area}/{market_type}/{datetime.today().strftime("%Y-%m-%d")}_{frequency}_table.csv'

        print(f"Saving: {fname}")
        df_sorted.to_csv(fname, index=False)

        print("\n")

def collect_auction_market_data(start_date, end_date, sub_modality='DayAhead', auction='MRC'):
    for market_area in ['AT','BE','CH','DE-LU','DK1','DK2','FI','FR','GB','NL','NO1','NO2','NO3','NO4','NO5','PL','SE1','SE2','SE3','SE4']:
        # for market_area in ['NO1','NO2','NO3','NO4','NO5','PL','SE1','SE2','SE3','SE4']:
        df = pd.DataFrame()
        # for market_area in ['NO1']:
        for date in pd.date_range(start=start_date, end=end_date):
            trading_date = date
            if sub_modality == 'DayAhead':delivery_date = date+timedelta(days=1)
            elif sub_modality == 'Intraday':delivery_date = date
            else:
                raise ValueError(f"Sub-modality {sub_modality} not supported")

            trading_date_str = trading_date.strftime("%Y-%m-%d")
            delivery_date_str = delivery_date.strftime("%Y-%m-%d")
            print(f'auction {sub_modality} | {market_area} | {date} | {trading_date_str} -> {delivery_date_str}')

            url = f"https://www.epexspot.com/en/market-data?market_area={market_area}&auction={auction}&trading_date={trading_date_str}&delivery_date={delivery_date_str}&underlying_year=&modality=Auction&sub_modality={sub_modality}&technology=&data_mode=table&period=&production_period="

            df_i = fetch_auction_data(delivery_date_str,  url=url)
            df = pd.concat([df,df_i])

        if not df.empty:
            df_sorted = df.sort_values(by='date')

            if not os.path.isdir(f"./data/{market_area}"):
                os.mkdir(f"./data/{market_area}")
            if not os.path.isdir(f"./data/{market_area}/{sub_modality}_{auction}"):
                os.mkdir(f"./data/{market_area}/{sub_modality}_{auction}")

            temp_index = pd.DatetimeIndex(df['date'])
            frequency = temp_index.inferred_freq
            fname = f'./data/{market_area}/{sub_modality}_{auction}/{datetime.today().strftime("%Y-%m-%d")}_{frequency}_table.csv'

            print(f"Saving: {fname}")
            df_sorted.to_csv(fname, index=False)
        else:
            print(f"NO AUCTION DATA FOR area={market_area} sub_modality={sub_modality} auction={auction}")

        print("\n")


if __name__ == '__main__':
    end_date = pd.Timestamp(datetime.today(), tz='Europe/Brussels')
    start_date = end_date-timedelta(days=4)

    # collect data for continous market
    collect_continuous_market_data(start_date, end_date)

    # collect auction data
    collect_auction_market_data(start_date, end_date, sub_modality='DayAhead', auction='MRC')
    collect_auction_market_data(start_date, end_date, sub_modality='Intraday', auction='IDA1')
    collect_auction_market_data(start_date, end_date, sub_modality='Intraday', auction='IDA2')
    collect_auction_market_data(start_date, end_date, sub_modality='Intraday', auction='IDA3')


#
# def fetch_spot_data(date:pd.Timestamp) -> pd.DataFrame:
#     date_str = date.strftime("%Y-%m-%d")
#     print(date_str)
#
#     url = f"https://www.epexspot.com/en/market-data?market_area=DE&auction=&trading_date=&delivery_date={date_str}&underlying_year=&modality=Continuous&sub_modality=&technology=&data_mode=table&period=&production_period=&product=60"
#
#     # Fetch the webpage
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
#     }
#
#     # Fetch the webpage
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()  # Raises an HTTPError for bad responses
#
#     # Parse the HTML content
#     soup = BeautifulSoup(response.text, 'html.parser')
#
#     # Find the table - adjust the selector as needed based on the actual table structure
#     table = soup.find('table')
#
#     # Convert the table to a DataFrame, if a table is found
#     data_frame = pd.read_html(str(table))[0] if table else None
#
#     # Display the DataFrame
#     data_frame = pd.DataFrame(data_frame)
#
#     # get hourly data (skip 15 min intervals)
#     data_frame_lim = data_frame.iloc[::7]
#
#     # add timestamp column
#     index_hours = [
#         "00 - 01", "01 - 02", "02 - 03", "03 - 04", "04 - 05", "05 - 06",
#         "06 - 07", "07 - 08", "08 - 09", "09 - 10", "10 - 11", "11 - 12",
#         "12 - 13", "13 - 14", "14 - 15", "15 - 16", "16 - 17", "17 - 18",
#         "18 - 19", "19 - 20", "20 - 21", "21 - 22", "22 - 23", "23 - 24"
#     ]
#
#     # Convert to Pandas Series
#     series_hours = pd.Series(index_hours)
#
#     # Function to parse the hour and convert to timestamp
#     def parse_hour_to_timestamp(hour_str):
#         hour = int(hour_str.split(' - ')[0])  # Split the string and convert the first part to an integer
#         timestamp = pd.to_datetime(f"{date_str} {hour}:00:00")  # Create a timestamp string and convert to datetime
#         return timestamp
#
#     # Apply the function to each item in the series
#     # data_frame_lim = series_hours.apply(parse_hour_to_timestamp)
#
#     # Display the resulting timestamps
#     data_frame_lim.index = series_hours.apply(parse_hour_to_timestamp)
#     #.reset_index(inplace=True)
#     data_frame_lim=data_frame_lim.reset_index().rename(columns={'index': 'date'})
#     #data_frame_lim.reset_index(inplace=True)
#
#     # map the column names for easy processing
#     name_map = {
#         'Low (€/MWh)': 'low',
#         'High (€/MWh)': 'high',
#         'Last (€/MWh)': 'last',
#         'Weight Avg. (€/MWh)': 'weighted_avg',
#         'ID Full (€/MWh)': 'id_full',
#         'ID1 (€/MWh)': 'id1',
#         'ID3 (€/MWh)': 'id3',
#         'Buy Volume (MWh)': 'buy_volume',
#         'Sell Volume (MWh)': 'sell_volume',
#         'Volume (MWh)': 'total_volume',
#     }
#     data_frame_lim = data_frame_lim.rename(columns=name_map)
#
#     return data_frame_lim
#
#
# if __name__ == '__main__':
#
#     # start_date = pd.Timestamp(datetime(year=2024, month=8, day=25), tz='Europe/Brussels')
#     end_date = pd.Timestamp(datetime.today(), tz='Europe/Brussels')#pd.Timestamp(datetime(year=2024, month=9, day=2), tz='Europe/Brussels')
#     start_date = end_date-timedelta(days=4)
#     # start_date = start_date.tz_localize(tz='Europe/Brussels')
#
#     df = pd.DataFrame()
#
#     for date in pd.date_range(start=start_date, end=end_date):
#         df_i = fetch_spot_data(date)
#         df = pd.concat([df,df_i])
#
#     df_sorted = df.sort_values(by='date')
#
#     df_sorted.to_csv('data/de_spot_' + datetime.today().strftime('%Y-%m-%d') + '.csv')
