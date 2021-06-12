import os
import time
import datetime
import pandas as pd
from version import print_version
from utils import get_db_manager

pickle_cache_file = 'data/tmp.pkl'


def check_diff(group_delimiter = 60, minimun_earn_rate = 0.005, show_top=20, use_cache=True):

    df = load_data(use_cache)

    # df_bk = df.copy()
    # df = df.head(200)
    # df = df[df['symbol']=='btc_jpy']
    # df = df_bk.copy()

    latest = datetime.datetime.fromtimestamp(df.timestamp.max())
    print(f"Latest data obtained at: {latest}")
    print(f"Number of records: {df.shape[0]:d}")
    print(df.head(5))
    
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
    # df_engouth_earn_rate.sort_values(by=['diff_ratio'], ascending=False, inplace=True)
    
    print(df_engouth_earn_rate.head(n=show_top))
    df_engouth_earn_rate.to_csv('data/price_diff.csv', index=False)

    return df_engouth_earn_rate

def load_data(use_cache=True):
    if use_cache and os.path.exists(pickle_cache_file):
        start = time.time()
        df = pd.read_pickle(pickle_cache_file)
        elsapsed = time.time() - start
        print(f"Reading from pickle cache took: {elsapsed:.3f}s")
    else:
        with get_db_manager() as db:
            # sql = "SELECT * FROM orderbook INNER JOIN depth ON orderbook.id=depth.orderbook_id;"
            sql = "SELECT * FROM orderbook;"
            start = time.time()
            df = pd.read_sql_query(sql=sql, con=db.conn)
            elsapsed = time.time() - start
            print(f"Reading from db took: {elsapsed}s")
            df.to_pickle(pickle_cache_file)
    return df


def main():
    print_version()
    check_diff()


if __name__ == '__main__':
    main()
