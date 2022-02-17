import time
import sys
import pandas as pd
import json
import requests
import pymysql
import datetime
import holidays
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from decouple import config
from datetime import date, timedelta

def get_jsonparsed_data(url):
    """
    Sends a GET request to API and returns the resulting data in a dictionary
    """
    # sending get request and saving the response as response object
    response = requests.get(url=url)
    data = json.loads(response.text)
    return data


def create_unique_id(df):
    """
    Creates unique_id used in database as primary key
    """
    # Create unique identifier and append to list
    id_list = []
    for idx, row in df.iterrows():
        symbol = row["symbol"]
        date = str(row["date"])
        unique_id = date + '-' + symbol
        id_list.append(unique_id)
    # Insert IDs into dataframe as new column
    df.insert(0, "id", id_list)
    return df


def clean_earnings_data(df):
    """
    Clean earnings data by:
    - Filtering out ADRs and other exchanges
    - Removing stocks that have any null values in epsEstimated, or time
    - Dropping revenue and revenueEstimated columns
    - Creating a unique ID
    - Changing date format
    """
    # If ticker is greater than a length of 5, drop it
    df["length"] = df.symbol.str.len()
    df = df[df.length < 5]
    # Filter missing columns out
    df = df.dropna(subset=['date', 'symbol', 'epsEstimated', 'time'])
    # Drop unwanted columns
    df = df.drop(['revenue', 'revenueEstimated', 'length'], axis=1)
    df = create_unique_id(df)
    df = df.rename({'date': 'earnings_date',
                   'epsEstimated': 'eps_estimated', 'time': 'earnings_time'}, axis=1)
    df["earnings_date"] = pd.to_datetime(
        df["earnings_date"]).dt.strftime('%m/%d/%y')
    return df


def clean_pricing_data(df, today):
    """
    Clean pricing data by:
    - Adding one day to earnings_date
    - Removing label column
    - Creating a unique ID
    - Changing date format
    """
    df.loc[:,'date'] = today
    df = df.drop(['label'], axis=1)
    df = create_unique_id(df)
    df = df.rename({'date': 'earnings_date', 'open': 'open_price', 'high': 'high_price', 'low': 'low_price',
                    'close': 'close_price', 'adjClose': 'adj_close', 'volume': 'daily_volume',
                    'unadjustedVolume': 'unadjusted_volume', 'change': 'change_dollars',
                    'changePercent': 'change_percent', 'changeOverTime': 'change_over_time'}, axis=1)
    df["earnings_date"] = pd.to_datetime(
        df["earnings_date"]).dt.strftime('%m/%d/%y')
    return df


def clean_technical_data(df):
    """
    Clean technical data by:
    - Renaming columns
    - Changing date format
    """
    df = create_unique_id(df)
    df = df.rename({'date': 'earnings_date', 0: 'sma_5', 1: 'sma_10', 2: 'sma_20', 3: 'ema_5',
                   4: 'ema_10', 5: 'ema_20', 6: 'rsi_14', 7: 'wma_5', 8: 'wma_10', 9: 'wma_20'}, axis=1)
    df["earnings_date"] = pd.to_datetime(
        df["earnings_date"]).dt.strftime('%m/%d/%y')
    return df

# Check if dataframe is empty, exit if so
def check_dataframe_empty(df, today):
    if df.empty:
        sys.exit("{}: No earnings available".format(today))

# Verify the integrity of dates by checking if its a business day and not a holiday
def verify_dates(today, last_day):
    us_holidays = holidays.US()
    if today in us_holidays:
        sys.exit("{}: U.S. holiday. Exiting program.".format(today))
    elif last_day in us_holidays:
        last_day = find_true_last_day(last_day, us_holidays)
        print("{}: Changed last_day to {}.".format(today, last_day))
    return today, last_day

# Recursive function that finds the true last business day, taking into account U.S. holidays
def find_true_last_day(last_day, us_holidays):
    if last_day in us_holidays:
        temp_last_day = str((pd.to_datetime(last_day) - pd.tseries.offsets.BusinessDay(n=1)).date())
        return find_true_last_day(temp_last_day, us_holidays)
    else:
        return last_day


if __name__ == "__main__":
    start = time.time()
    # Connect to boto3 and pull parameters
    client = boto3.client('ssm')
    response = client.get_parameters(Names=[
            "/rds-pipelines/dev/aws-db-name",
            "/rds-pipelines/dev/aws-key",
            "/rds-pipelines/dev/aws-port",
            "/rds-pipelines/dev/aws-user",
            "/rds-pipelines/dev/db-url",
            "/rds-pipelines/dev/fmp-cloud-key",
            "/rds-pipelines/dev/fmp-key"
    ])
    
    aws_database = response['Parameters'][0]['Value']
    aws_password = response['Parameters'][1]['Value']
    aws_port = response['Parameters'][2]['Value']
    aws_username = response['Parameters'][3]['Value']
    aws_hostname = response['Parameters'][4]['Value']

    # Pull API keys from .env file
    FMP_CLOUD_API_KEY = response['Parameters'][5]['Value']
    FMP_API_KEY = response['Parameters'][6]['Value']

    # Find today's and the last business day's date
    today = str(date.today())
    last_day = str((date.today() - pd.tseries.offsets.BusinessDay(n=1)).date())
    today, last_day = verify_dates(today, last_day)

    # Find which day of the week it is
    weekno = datetime.datetime.today().weekday()
    # Exit program if it is the weekend (no eanings/pricing data)
    if weekno >= 5:
        sys.exit("{}: No data available on the weekend".format(today))

    print("{}: Beginning data pull...".format(today))
    # Setup SQL Alchemy for AWS database
    sqlalch_conn = "mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format(
        aws_username, aws_password, aws_hostname, aws_database)
    engine = create_engine(sqlalch_conn, echo=False)

    # Connect to FMP API and pull earnings data
    earnings_res = get_jsonparsed_data(
        "https://financialmodelingprep.com/api/v3/earning_calendar?from={}&to={}&apikey={}".format(today, today, FMP_API_KEY))
    earnings_df = pd.DataFrame(earnings_res)
    check_dataframe_empty(earnings_df, today)

    # Filter earnings data
    earnings_filtered = clean_earnings_data(earnings_df)
    check_dataframe_empty(earnings_filtered, today)

    try:
        earnings_filtered.to_sql(
            "earnings", con=engine, index=False, if_exists='append')
    except Exception as e:
        print("Earnings data already exists in table")

    # Pull list of symbols
    symbols = earnings_filtered.symbol
    print("Gathering data for {} earnings reports...".format(len(symbols)))

    # For each symbol pull today's pricing
    pricing_df = pd.DataFrame()
    for symbol in symbols:
        url = "https://financialmodelingprep.com/api/v3/historical-price-full/{}?from={}&to={}&apikey={}".format(
            symbol, last_day, last_day, FMP_API_KEY)
        res = get_jsonparsed_data(url)
        try:
            price_res_df = pd.DataFrame.from_records(res["historical"])
            # Insert symbol
            price_res_df.insert(1, "symbol", symbol)
            # Concat with main dataframe
            pricing_df = pd.concat([pricing_df, price_res_df])
        except KeyError as ke:
            print("Skipping symbol: {}. Error message: {}".format(symbol,ke))

    # Filter pricing data
    pricing_filtered = clean_pricing_data(pricing_df, today)

    try:
        pricing_filtered.to_sql(
            "pricing", con=engine, index=False, if_exists='append')
    except Exception as e:
        print("Pricing data already exists in table")

    indicators = ["sma_5", "sma_10", "sma_20", "ema_5", "ema_10",
                  "ema_20", "rsi_14", "wma_5", "wma_10", "wma_20"]

    # Pull technical indicators for each stock in today's earnings list
    technical_df = pd.DataFrame()
    for symbol in symbols:
        technical_list = []
        for indicator in indicators:
            # Gather functions and periods for API request
            func, period = indicator.split("_")
            url = "https://fmpcloud.io/api/v3/technical_indicator/daily/{}?period={}&type={}&apikey={}".format(
                symbol, period, func, FMP_CLOUD_API_KEY)
            try:
                tech_res = get_jsonparsed_data(url)
                tech_res_df = pd.DataFrame(tech_res)
                # Select indicator column for appropriate date
                select = ((tech_res_df.loc[(tech_res_df["date"] == last_day)])[func]).iloc[0]
                technical_list.append(select)
            except IndexError as ie: 
                print("Skipping symbol: {}. Error message: {}".format(symbol,ie))
        # Append list to be rows in dataframe
        technical_series = pd.Series(technical_list)
        technical_df = technical_df.append(technical_series, ignore_index=True)
    symbol_series = pd.Series(symbols)
    technical_df.insert(0, "symbol", symbol_series.values)
    technical_df.insert(1, "date", today)
    technical_filtered = clean_technical_data(technical_df)

    try:
        technical_filtered.to_sql(
            "technicals", con=engine, index=False, if_exists='append')
    except Exception as e:
        print("Technical data already exists in table")

    end = time.time() - start
    print("{}: Successful execution. Execution time: {}".format(today, end))
