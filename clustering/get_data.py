
import os
import json
import datetime

import pandas as pd


path = os.path.dirname(os.path.abspath(__file__))


def get_exchange_rate_block() -> dict:

    with open(os.path.join(path, "data", "market-price.json")) as f:
        price = json.load(f)["market-price"]
        price = pd.DataFrame(price)
        price.columns = ["date", "price"]
        price["date"] = price["date"].apply(lambda x: datetime.datetime.fromtimestamp(x // 1000))
        price = price.set_index("date")
        price = price.loc[(price.price > 0) & (price.index >= datetime.datetime(2012, 1, 1)), "price"]

    with open(os.path.join(path, "data", "block-dates.json")) as f:
        blocks = json.load(f)
        blocks = {datetime.datetime.fromtimestamp(v): int(k) for k, v in blocks.items()}
        blocks = pd.Series(blocks, name="index")

    block_price = pd.concat([price, blocks], axis=1).ffill().dropna()
    block_price = block_price.reset_index(drop=True).groupby("index").mean()
    block_price = block_price.reindex(
        pd.RangeIndex(start=block_price.index.min(), stop=block_price.index.max() + 1)).ffill()

    return dict(block_price["price"])




