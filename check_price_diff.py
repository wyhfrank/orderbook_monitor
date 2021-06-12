import os
import time
import datetime
import pickle
import pandas as pd
from version import print_version
from utils import get_db_manager

pickle_cache_file = 'data/tmp.pkl'
default_output_dir = 'data/calcs'
last_db_access_file = 'last_db_access.txt'


def check_diff(date_range='auto', exclued_exchange=[], group_delimiter = 60, minimun_earn_rate = 0.005, show_top=20, use_cache=True):

    df = load_data(date_range=date_range, use_cache=use_cache)

    # df_bk = df.copy()
    # df = df.head(200)
    # df = df[df['symbol']=='btc_jpy']
    # df = df_bk.copy()

    oldest = datetime.datetime.fromtimestamp(df.timestamp.min())
    latest = datetime.datetime.fromtimestamp(df.timestamp.max())
    print(f"Time range: {oldest} -- {latest}")
    print(f"Number of records: {df.shape[0]:d}")
    print(df.tail(5))

    # Exclude exchange
    if exclued_exchange:
        df = df[~df['exchange'].isin(exclued_exchange)].copy()
    
    # Group by symbol and timestamp
    # https://stackoverflow.com/a/30244979/1938012
    grouped = df.groupby(['symbol', 'timestamp'], sort=False)

    # Find the exchange (sell_to) that has the highest bid within the group
    # https://stackoverflow.com/a/32459442
    sell_to = df.loc[grouped['best_bid'].idxmax()].rename(columns={'exchange':'sell_to', 'best_bid': 'sell_price'})
    sell_to = sell_to[['symbol', 'timestamp', 'sell_to', 'sell_price']]

    # Join the sell_to side to the original dataframe
    df = df.merge(sell_to)
    # Rename the exchange with low ask price to buy_from
    df = df.rename(columns={'exchange':'buy_from', 'best_ask': 'buy_price'})

    # Calculate the price diff and remove those without any potential profit
    df['price_diff'] = df['sell_price'] - df['buy_price']
    # https://stackoverflow.com/a/53954986/1938012
    # Use a copy to suppress the warning SettingWithCopyWarning
    df_diff_over_0 = df[df['price_diff'] > 0].copy()
    df_diff_over_0['diff_ratio'] = df_diff_over_0['price_diff'] / df_diff_over_0['buy_price']

    # Keep only one record that has the highest diff_ratio for each symbol
    df_diff_over_0.sort_values(by=['timestamp', 'diff_ratio'], ascending=[True, False], inplace=True)
    df_highest_diff_in_group = df_diff_over_0.groupby(['symbol', 'timestamp'], sort=False).first().reset_index()

    # Merge consecutive rows
    # https://stackoverflow.com/a/46732998/1938012
    
    # If two consecutive records are larger than this time interval (in seconds), 
    #  they are treated as seperate opportunity
    df_independents = df_highest_diff_in_group.groupby(['symbol'], sort=False).apply(lambda gdf: 
        gdf.groupby(((gdf.timestamp  - gdf.timestamp.shift(1)) > group_delimiter).cumsum())
        .first()).droplevel([0])
    # df_independents.sort_values(by='diff_ratio', ascending=False, inplace=True)

    df_engouth_earn_rate = df_independents[df_independents['diff_ratio'] >= minimun_earn_rate].copy()
    df_engouth_earn_rate.sort_values(by=['diff_ratio'], ascending=False, inplace=True)
    
    print(df_engouth_earn_rate.head(n=show_top))
    df_engouth_earn_rate.to_csv(get_output_fn(oldest, latest), index=False)

    return df_engouth_earn_rate


def load_data(date_range='auto', use_cache=True):
    """ Load data within certain date range, either from database or cache file.
        If use_cache is True, data_range is ignored.

        Possible options for date_range: 
            - "auto" (default): start from last db query, end with now()
            - "full": everything from db
            - (start, end): a tuple/list of two datetime object
    """

    if use_cache and os.path.exists(pickle_cache_file):
        start = time.time()
        df = pd.read_pickle(pickle_cache_file)
        elsapsed = time.time() - start
        print(f"Reading from pickle cache took: {elsapsed:.3f}s")
    else:
        if date_range == "all":
            sql = "SELECT * FROM orderbook;"
        else:
            if date_range == "auto":
                start_date = get_latest_date()
                if not start_date:
                    start_date = datetime.datetime(2000, 1, 1)
                end_date = datetime.datetime.now()
            elif not isinstance(date_range, str) and len(date_range) > 1:
                start_date, end_date = date_range[0:2]
            try:
                start_ts, end_ts = list(map(lambda x: int(x.timestamp()),(start_date, end_date)))
            except AttributeError:
                raise TypeError("Date range need to be datetime type.")
            sql = f"SELECT * FROM orderbook WHERE timestamp>={start_ts} and timestamp<={end_ts};"

        # print(sql)
        with get_db_manager() as db:
            start = time.time()
            df = pd.read_sql_query(sql=sql, con=db.conn)
            elsapsed = time.time() - start
            print(f"Reading from db took: {elsapsed:.3f}s")
            
            # Save data to cache file
            df.to_pickle(pickle_cache_file)

            # Save last db access info
            latest = datetime.datetime.fromtimestamp(df.timestamp.max())
            save_latest_date(last_access_date=latest)
    return df


def get_latest_date(path=default_output_dir):
    try:
        with open(os.path.join(path, last_db_access_file), 'rb') as f:
            last_access_date = pickle.load(f)
    except FileNotFoundError:
        return None
    return last_access_date


def save_latest_date(last_access_date, path=default_output_dir):
    with open(os.path.join(path, last_db_access_file), 'wb') as f:
        pickle.dump(last_access_date, f)


def get_output_fn(start_date=None, end_date=None, path=default_output_dir):
    if not start_date: start_date = datetime.datetime.now()
    if not end_date: end_date = datetime.datetime.now()

    fmt = '%Y-%m-%d-%H-%M-%S'
    start_str = start_date.strftime(fmt)
    end_str = end_date.strftime(fmt)

    fn = os.path.join(path, f"potential_earn_{start_str}__{end_str}.csv")

    if not os.path.exists(path):
        os.makedirs(path)
    
    return fn


def main():
    print_version()
    use_cache=True
    use_cache=False
    exclued_exchange = ['bitflyer']

    check_diff(use_cache=use_cache, exclued_exchange=exclued_exchange)


if __name__ == '__main__':
    main()
