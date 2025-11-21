"""
Updated version of the scraper that now utilizes headless selenium browser to bypass JS restriction.
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from logger import get_logger

logger = get_logger(__name__)

def build_epex_url(
    market_area: str,
    delivery_date: datetime,
    auction: str = "MRC",
    modality: str = "Auction",
    sub_modality: str = "DayAhead",
    data_mode: str = "table",
    period: str = "",
    production_period: str = "",
) -> str:
    """
    Build an EPEX market-results URL matching what you showed.
    Adjust trading_date / delivery_date logic if needed.
    """
    d_str_query = delivery_date.strftime("%Y-%m-%d")  # for query params
    # Many pages simply use the same date for trading & delivery; adjust if you need +1 day etc.
    t_str_query = d_str_query

    base = "https://www.epexspot.com/en/market-results"
    params = (
        f"?market_area={market_area}"
        f"&auction={auction}"
        f"&trading_date={t_str_query}"
        f"&delivery_date={d_str_query}"
        f"&underlying_year="
        f"&modality={modality}"
        f"&sub_modality={sub_modality}"
        f"&technology="
        f"&data_mode={data_mode}"
        f"&period={period}"
        f"&production_period={production_period}"
    )
    logger.info(f"Building EPEX market-results URL: {base}{params}")
    return base + params


def _parse_number(text: str) -> float:
    """
    Parse European style numeric strings like '2,914.6' or '732.6' to float.
    Here the comma is thousands separator and '.' is decimal separator.
    """
    text = text.strip()
    if not text:
        return float("nan")
    # Remove spaces and thousands separator commas
    text = text.replace(" ", "").replace(",", "")
    return float(text)


def create_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Create and return a Chrome WebDriver using Selenium Manager.
    """
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    return driver


def accept_cookies_if_present(driver, timeout: int = 5):
    """
    Try to close/accept cookie banner.
    This is best-effort and ignores failure if nothing is found.
    You may need to adapt the locators to whatever EPEX uses at the moment.
    """
    try:
        wait = WebDriverWait(driver, timeout)
        # Common patterns; adjust as necessary
        possible_locators = [
            (By.ID, "onetrust-accept-btn-handler"),
            (By.CSS_SELECTOR, "button[aria-label*='Accept'][aria-label*='cookies']"),
            (By.XPATH, "//button[contains(., 'Accept') and contains(., 'cookie')]"),
            (By.XPATH, "//button[contains(., 'Accept all')]"),
        ]
        for by, sel in possible_locators:
            try:
                btn = wait.until(EC.element_to_be_clickable((by, sel)))
                btn.click()
                return
            except TimeoutException:
                continue
    except Exception:
        # Silently ignore any cookie-handling errors
        pass


def wait_for_table_loaded(driver, timeout: int = 30):
    """
    Wait until the spinner disappears and the table rows are available.
    """
    wait = WebDriverWait(driver, timeout)

    # 1) Wait until the JS widget container is present
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-md-widget")))

    # 2) Optionally wait until any loading spinner disappears
    try:
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".js-md-spinner")))
    except TimeoutException:
        # Spinner might not be present or may never fully disappear; ignore in that case
        pass

    # 3) Finally, wait until we see at least one data row in tbody
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div.js-table-values table tbody tr.child")
        )
    )


def scrape_epex_day_ahead(
    market_area: str,
    auction:str,
    sub_modality: str,
    delivery_date: datetime,
    headless: bool = True,
) -> List[Dict]:
    """
    Scrape day-ahead prices and volumes from EPEX SPOT "Market Results" page
    for a given market area and delivery date.

    Returns: list of dicts with keys:
        - 'time_interval'
        - 'buy_volume_mwh'
        - 'sell_volume_mwh'
        - 'volume_mwh'
        - 'price_eur_mwh'
    """
    logger.info(f"Processing auction {auction} | {sub_modality} | {market_area} | {delivery_date.strftime('%Y-%m-%d')}")

    url = build_epex_url(
        market_area=market_area,
        delivery_date=delivery_date,
        auction = auction,
        modality = "Auction",
        sub_modality = sub_modality,
        data_mode = "table",
        period = "",
        production_period=""
    )

    driver = create_driver(headless=headless)
    try:
        driver.get(url)

        # Handle cookie popup if it appears
        accept_cookies_if_present(driver)

        # Wait until table is loaded
        wait_for_table_loaded(driver)

        # ---- Extract time intervals from the left fixed column ----
        time_elems = driver.find_elements(
            By.CSS_SELECTOR, "div.fixed-column.js-table-times ul li.child a"
        )
        time_intervals = [el.text.strip() for el in time_elems]

        # ---- Extract table rows (volumes & prices) ----
        row_elems = driver.find_elements(
            By.CSS_SELECTOR, "div.js-table-values table tbody tr.child"
        )

        if not row_elems:
            raise RuntimeError("No data rows found (tr.child). The page structure may have changed.")

        if len(time_intervals) != len(row_elems):
            # Not fatal, but good to know
            logger.warning(
                f"number of time intervals ({len(time_intervals)}) "
                f"!= number of data rows ({len(row_elems)})."
            )

        data = []
        for t, row in zip(time_intervals, row_elems):
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]

            # Expected in HTML: <td>Buy Volume</td><td>Sell Volume</td><td>Volume</td><td>Price</td>
            if len(cols) < 4:
                # Structure changed? Log and skip this row
                logger.warning(f"Skipping row with unexpected number of columns ({len(cols)}): {cols}")
                continue

            buy_vol = _parse_number(cols[0])
            sell_vol = _parse_number(cols[1])
            volume = _parse_number(cols[2])
            price = _parse_number(cols[3])

            data.append(
                {
                    "time_interval": t,
                    "buy_volume_mwh": buy_vol,
                    "sell_volume_mwh": sell_vol,
                    "volume_mwh": volume,
                    "price_eur_mwh": price,
                }
            )
            logger.debug(f"Found {len(data)} data rows ({len(data)}).")

        return data

    finally:
        driver.quit()


def results_to_dataframe(results:list[dict], delivery_date:datetime.date) -> pd.DataFrame:
    """
    Convert the scraped `results` list into a pandas DataFrame.

    Parameters
    ----------
    results : list[dict]
        Output of `scrape_epex_day_ahead`, e.g.
        [
          {
            'time_interval': '00:00 - 00:15',
            'buy_volume_mwh': 130.0,
            'sell_volume_mwh': 216.4,
            'volume_mwh': 216.4,
            'price_eur_mwh': 93.33
          },
          ...
        ]
    delivery_date : datetime.date or datetime.datetime
        The delivery date (`d` in your example). The time from the
        first part of `time_interval` is added to this date.

    Returns
    -------
    pandas.DataFrame
        Columns: ['date', 'Buy_Volume', 'Sell_Volume', 'Volume', 'Price']
    """

    # Ensure we have a plain date object
    if isinstance(delivery_date, datetime):
        base_date = delivery_date.date()
    elif isinstance(delivery_date, datetime.date):
        base_date = delivery_date
    else:
        raise TypeError(
            "delivery_date must be a datetime.date or datetime.datetime, "
            f"got {type(delivery_date)}"
        )

    rows = []
    for row in results:
        # '00:00 - 00:15' -> '00:00'
        interval = row["time_interval"]
        start_time_str = interval.split("-")[0].strip()

        # Build full datetime: delivery_date + start time
        start_time = datetime.strptime(start_time_str, "%H:%M").time()
        dt = datetime.combine(base_date, start_time)

        rows.append(
            {
                "date": dt,
                "Buy_Volume": row["buy_volume_mwh"],
                "Sell_Volume": row["sell_volume_mwh"],
                "Volume": row["volume_mwh"],
                "Price": row["price_eur_mwh"],
            }
        )

    df = pd.DataFrame(rows)
    # Just in case: sort by datetime
    df = df.sort_values("date").reset_index(drop=True)
    return df

def process_market(market_area:str="DE-LU", auction:str="IDA1", sub_modality:str="Intraday", days_to_look_back:int=3):
    """Main scrape function."""
    end_date = pd.Timestamp(datetime.today(), tz="Europe/Brussels").date()
    if sub_modality == "DayAhead":
        end_date = end_date + timedelta(days=1) # for Day-Ahead
    start_date = end_date-timedelta(days=days_to_look_back)

    df = pd.DataFrame()
    # for market_area in ['NO1']:
    for delivery_date in pd.date_range(start=start_date, end=end_date):
        try:
            logger.info(f"Starting scraping {market_area} | {auction} | {sub_modality} for delivery date={delivery_date}")
            results:list[Dict] = scrape_epex_day_ahead(market_area=market_area, auction=auction, delivery_date=delivery_date, sub_modality=sub_modality, headless=True)
        except Exception as e:
            logger.error(f"Failed to scrape {market_area} | {auction} | {sub_modality} for delivery date={delivery_date} with Error: \n{e}")
            results = []
        logger.info(f"Adding {len(results)} rows to the dataframe for this date.")
        result_df = results_to_dataframe(results=results, delivery_date=delivery_date) if len(results) > 0 else pd.DataFrame()
        df = pd.concat([df,result_df]) # Add this day results to the dataframe

    if not df.empty:
        df_sorted = df.sort_values(by='date')

        os.makedirs(f"./data/{market_area}", exist_ok=True)

        os.makedirs(f"./data/{market_area}/{sub_modality}_{auction}", exist_ok=True)

        temp_index = pd.DatetimeIndex(df['date'])
        frequency = temp_index.inferred_freq
        fname = f'./data/{market_area}/{sub_modality}_{auction}/{datetime.today().strftime("%Y-%m-%d")}_{frequency}_table.csv'

        logger.info(f"Saving: {fname}")
        df_sorted.to_csv(fname, index=False)

if __name__ == "__main__":

    for market_area in [
        'AT','BE','CH','DE-LU','DK1','DK2','FI','FR','GB','NL','NO1','NO2','NO3','NO4','NO5','PL','SE1','SE2','SE3','SE4'
    ]:
        process_market(market_area=market_area, sub_modality='DayAhead', auction='MRC')
        process_market(market_area=market_area, sub_modality='Intraday', auction='IDA1')
        process_market(market_area=market_area, sub_modality='Intraday', auction='IDA2')
        process_market(market_area=market_area, sub_modality='Intraday', auction='IDA3')