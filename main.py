from datetime import datetime,timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd

def fetch_spot_data(date:pd.Timestamp) -> pd.DataFrame:
    date_str = date.strftime("%Y-%m-%d")
    print(date_str)

    url = f"https://www.epexspot.com/en/market-data?market_area=DE&auction=&trading_date=&delivery_date={date_str}&underlying_year=&modality=Continuous&sub_modality=&technology=&data_mode=table&period=&production_period=&product=60"

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

    # Display the DataFrame
    data_frame = pd.DataFrame(data_frame)

    # get hourly data (skip 15 min intervals)
    data_frame_lim = data_frame.iloc[::7]

    # add timestamp column 
    index_hours = [
        "00 - 01", "01 - 02", "02 - 03", "03 - 04", "04 - 05", "05 - 06",
        "06 - 07", "07 - 08", "08 - 09", "09 - 10", "10 - 11", "11 - 12",
        "12 - 13", "13 - 14", "14 - 15", "15 - 16", "16 - 17", "17 - 18",
        "18 - 19", "19 - 20", "20 - 21", "21 - 22", "22 - 23", "23 - 24"
    ]

    # Convert to Pandas Series
    series_hours = pd.Series(index_hours)

    # Function to parse the hour and convert to timestamp
    def parse_hour_to_timestamp(hour_str):
        hour = int(hour_str.split(' - ')[0])  # Split the string and convert the first part to an integer
        timestamp = pd.to_datetime(f"{date_str} {hour}:00:00")  # Create a timestamp string and convert to datetime
        return timestamp

    # Apply the function to each item in the series
    # data_frame_lim = series_hours.apply(parse_hour_to_timestamp)

    # Display the resulting timestamps
    data_frame_lim.index = series_hours.apply(parse_hour_to_timestamp)
    #.reset_index(inplace=True)
    data_frame_lim=data_frame_lim.reset_index().rename(columns={'index': 'date'})
    #data_frame_lim.reset_index(inplace=True)

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
    }
    data_frame_lim = data_frame_lim.rename(columns=name_map)

    return data_frame_lim


if __name__ == '__main__':

    # start_date = pd.Timestamp(datetime(year=2024, month=8, day=25), tz='Europe/Brussels')
    end_date = pd.Timestamp(datetime.today(), tz='Europe/Brussels')#pd.Timestamp(datetime(year=2024, month=9, day=2), tz='Europe/Brussels')
    start_date = end_date-timedelta(days=4)
    # start_date = start_date.tz_localize(tz='Europe/Brussels')

    df = pd.DataFrame()
    
    for date in pd.date_range(start=start_date, end=end_date):
        df_i = fetch_spot_data(date)
        df = pd.concat([df,df_i])
        
    df_sorted = df.sort_values(by='date')
    
    df_sorted.to_csv('data/de_spot_' + datetime.today().strftime('%Y-%m-%d') + '.csv')
