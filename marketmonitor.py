import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import multiprocessing
from multiprocessing import Pool

# You are provided a personal key when you sign up for Polygon (also free tier)
polykey = "PUT YOUR POLYGON KEY HERE"

# choose exchange XNYS for NYSE or XNAS for NASDAQ
exchange = 'XNYS'

# define start/end date to generate market monitor for
# you need to load a minimum of 65 trading days
start_date = datetime(2023, 1, 1)  # April 1, 2024
end_date = datetime(2024, 4, 12)   # April 10, 2024

def iterate_over_weekdays(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday = 0, Sunday = 6
            yield current_date
        current_date += timedelta(days=1)

def process_day(day):
    print(f'Processing {day}')
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day.strftime("%Y-%m-%d")}?adjusted=true&apiKey={polykey}'
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = json.loads(response.text)
    if not data or 'results' not in data:
        return None
    df = pd.json_normalize(data, record_path=['results'])
    df = df[['T','v','o','h','l','c']]
    df['Date'] = day
    return df

def process_tickers():
    print('Loading tickers from Polygon.. can take up to 30 seconds')
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&apiKey={polykey}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = json.loads(response.text)
        
        if not data or 'results' not in data:
            return pd.DataFrame()

    except:
        return pd.DataFrame()

    df_list = []
    while True:
        try:
            df = pd.json_normalize(data, record_path=['results'])
            df = df[['ticker','type','primary_exchange']]
        except:
            return pd.DataFrame()
        
        df_list.append(df)

        if 'next_url' in data:
            next_url = data['next_url'] + '&apiKey=' + polykey
            try:
                response = requests.get(next_url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                data = json.loads(response.text)
            except:
                break
        else:
            break
    
    if df_list:
        df = pd.concat(df_list, axis=0, ignore_index=True)
        return df
    else:
        return pd.DataFrame()
    
def process_data(df_combined):
    df_combined['dolvol'] = df_combined['c'] * df_combined['v']

    df_combined = pd.merge(df_combined, df_tickers, left_on='T', right_on='ticker', how='inner')

    df_combined['Date'] = pd.to_datetime(df_combined['Date'])

    df_combined = df_combined.sort_values(['Date', 'T'], ascending=[True, True])

    df_combined['PctChange'] = df_combined.groupby('T')['c'].pct_change()

    df_temp = df_combined.groupby('T')['c'].rolling(20, 1).mean().to_frame('MA020').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['dolvol'].rolling(20, 1).mean().to_frame('dolvol020').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['v'].shift(1).to_frame('pv').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['c'].shift(20).to_frame('pc020').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['l'].rolling(34, 1).min().to_frame('ll034').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['h'].rolling(34, 1).max().to_frame('hh034').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['l'].rolling(65, 1).min().to_frame('ll065').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['h'].rolling(65, 1).max().to_frame('hh065').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    df_temp = df_combined.groupby('T')['c'].rolling(40, 1).mean().to_frame('MA040').reset_index(0, drop=True)
    df_combined = pd.concat([df_combined, df_temp], axis=1)

    cond_minvol = (df_combined['v'] > 100000)
    cond_vol = (df_combined['v'] > df_combined['pv'])
    cond_liquid = (df_combined['dolvol020'] >= 250000)
    con_minprice = (df_combined['pc020'] >= 5)

    df_combined['t2108'] = (df_combined['c'] > df_combined['MA040']).astype(int)

    df_combined['IsUp4Pct'] = ((df_combined['PctChange'] >= 0.04) & (cond_minvol) & (cond_vol)).astype(int)
    df_combined['IsDn4Pct'] = ((df_combined['PctChange'] <= -0.04) & (cond_minvol) & (cond_vol)).astype(int)

    df_combined['IsUp25PctQtr'] = ((df_combined['c'] >= 1.25 * df_combined['ll065']) & (cond_liquid)).astype(int)
    df_combined['IsDn25PctQtr'] = ((df_combined['c'] <= 0.75 * df_combined['hh065']) & (cond_liquid)).astype(int)

    df_combined['IsUp25PctMnt'] = ((df_combined['c'] >= 1.25 * df_combined['pc020']) & (cond_liquid) & (con_minprice)).astype(int)
    df_combined['IsDn25PctMnt'] = ((df_combined['c'] <= 0.75 * df_combined['pc020']) & (cond_liquid) & (con_minprice)).astype(int)

    df_combined['IsUp50PctMnt'] = ((df_combined['c'] >= 1.5 * df_combined['pc020']) & (cond_liquid) & (con_minprice)).astype(int)
    df_combined['IsDn50PctMnt'] = ((df_combined['c'] <= 0.5 * df_combined['pc020']) & (cond_liquid) & (con_minprice)).astype(int)

    df_combined['IsUp13Pct34D'] = ((df_combined['c'] >= 1.13 * df_combined['ll034']) & (cond_liquid)).astype(int)
    df_combined['IsDn13Pct34D'] = ((df_combined['c'] <= 0.87 * df_combined['hh034']) & (cond_liquid)).astype(int)

    aggregate_df = df_combined.groupby('Date').agg({   'IsUp4Pct': 'sum', 
                                                        'IsDn4Pct': 'sum',
                                                        'IsUp25PctQtr': 'sum',
                                                        'IsDn25PctQtr': 'sum',
                                                        'IsUp25PctMnt': 'sum',
                                                        'IsDn25PctMnt': 'sum',
                                                        'IsUp50PctMnt': 'sum',
                                                        'IsDn50PctMnt': 'sum',
                                                        'IsUp13Pct34D': 'sum',
                                                        'IsDn13Pct34D': 'sum',
                                                        't2108': 'sum',
                                                        'T': 'count'
                                                        }).reset_index()
    
    aggregate_df['t2108 ratio'] = aggregate_df['t2108'] / aggregate_df['T']
    
    aggregate_df['5 day ratio'] = aggregate_df['IsUp4Pct'].rolling(5, 1).sum() / aggregate_df['IsDn4Pct'].rolling(5, 1).sum() 
    aggregate_df['10 day ratio'] = aggregate_df['IsUp4Pct'].rolling(10, 1).sum() / aggregate_df['IsDn4Pct'].rolling(10, 1).sum() 

    aggregate_df.sort_values(by=['Date'], ascending=False, inplace=True)
    
    aggregate_df = aggregate_df.iloc[:-65]

    return aggregate_df

if __name__ == '__main__':
    
    # load base ticker data to determine only common stocks to load
    df_tickers = process_tickers()
    df_tickers = df_tickers[df_tickers['type']=='CS'] # common stock
    # df_tickers = df_tickers[df_tickers['primary_exchange']==exchange] # common stock
    
    days_to_process = list(iterate_over_weekdays(start_date, end_date))
    
    # Get the number of available CPUs
    num_cpus = multiprocessing.cpu_count()
    
    # Calculate the number of worker processes to achieve 75% CPU usage
    target_cpu_usage = 0.75
    num_worker_processes = int(num_cpus * target_cpu_usage)
    
    with Pool(processes=num_worker_processes) as pool:
        results = pool.map(process_day, days_to_process)
        
    df_combined = pd.concat([df for df in results if df is not None], axis=0)
    
    df_combined.reset_index(drop=True, inplace=True)
    
    aggregate_df = process_data(df_combined)

    aggregate_df.to_csv('MarketMonitor.csv', index=False)
