import pandas as pd


data_file = 'tutorials/data.csv'

def test_split():
    df = pd.read_csv(data_file)

    grouped = df.groupby(['symbol', 'timestamp'])


    # https://stackoverflow.com/a/32459442
    sell_to = df.iloc[grouped['best_bid'].idxmax()].rename(columns={'exchange':'sell_to', 'best_bid': 'max_bid'})
    
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html#database-style-dataframe-or-named-series-joining-merging
    df = df.merge(sell_to)
    
    # df = df.assign(price_diff=df.max_bid-df.best_bid)
    # df = df.assign(ratio=df.price_diff/df.best_bid)

    df['price_diff'] = df['max_bid'] - df['best_bid']
    df = df.loc[df['price_diff'] >= 2]
    df['ratio'] = df['price_diff'] / df['best_bid']

    return df
    

if __name__ == '__main__':
    test_split()