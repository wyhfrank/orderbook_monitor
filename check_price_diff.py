import time
import datetime
import numpy as np
from dbmanager import DBManager
import pandas as pd
from version import print_version


def check_diff(show_top=20):
    db_path = 'db'
    db = DBManager(db_path=db_path)
    db.connect()

    sql = "SELECT * FROM orderbook;"
    df = pd.read_sql_query(sql=sql, con=db.conn)
    db.close()

    latest = datetime.datetime.fromtimestamp(df.timestamp.max())
    print(f"Latest data obtained at: {latest}")

    print(df.tail())

    # grouped = df.groupby(['symbol', 'timestamp'], as_index=False)
    grouped = df.groupby(['symbol', 'timestamp'], as_index=True)

    df_res = pd.DataFrame()

    # TODO: this iteration is too costy, find a faster way
    # https://www.yiibai.com/pandas/python_pandas_groupby.html
    # https://towardsdatascience.com/all-pandas-groupby-you-should-know-for-grouping-data-and-performing-operations-2a8ec1327b5

    print("Iterating thru each group...")
    start_time = time.time()

    for i, (name, group) in enumerate(grouped):
        if len(group) < 2:
            continue
        # print(name)
        # print(group)

        group["min_ask"] = group["best_ask"].agg(np.min)
        group["max_bid"] = group["best_bid"].agg(np.max)

        group["sell_to"] = group[group["best_bid"]==group["max_bid"]].iloc[0]["exchange"]
        group["buy_from"] = group["exchange"]
        group["price_diff"] = group.apply(lambda x: x["max_bid"] - x["best_ask"], axis=1)
        group["diff_ratio"] = group.apply(lambda x: x["price_diff"] / x["max_bid"], axis=1)

        df_res = df_res.append(group)

    elapsed_seconds = time.time() - start_time
    print(f"Finished iterating with {elapsed_seconds:.1f}s ({elapsed_seconds / 60:.1f}m).")

    df_res = df_res[df_res["price_diff"] > 0]

    df_res.sort_values(by="price_diff", ascending=False, inplace=True)
    cols = ["id", "symbol", "price_diff", "diff_ratio", "buy_from", "best_ask", "best_bid", "sell_to", "max_bid", "timestamp"]
    df_res = df_res[cols]

    print(df_res.head(n=show_top))
    df_res.to_csv('db/price_diff.csv', index=False)


def main():
    print_version()
    check_diff()


if __name__ == '__main__':
    main()
