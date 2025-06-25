
import numpy as np

import psycopg2
from database.dataService import DataService

from hierarchical_clustering.sampling import get_forbidden_transactions, sample_from_coinbase_transaction
from hierarchical_clustering.dbmodels.transactions import Transaction


def sample_transactions(db: dict, hc_db: dict, start: int, end: int,
                        exclude_coinjoin: bool = True,
                        exclude_colored_coin: bool = True,
                        seed: int = 0, num_starts: int = 1,
                        max_depth: int = None, max_transactions_depth: int = None):

    # if the table already exists skip this step
    table_name = Transaction.table_name(start=start, end=end)
    hc_ds = DataService(**hc_db)
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        row = hc_ds.execute_query(query=query, fetch="one")
        if row["count"] > 0:
            return
    except psycopg2.errors.UndefinedTable:
        pass

    hc_ds.execute_query(Transaction.create_table(start, end))

    forbidden = get_forbidden_transactions(db=db, start=start, end=end, exclude_coinjoin=exclude_coinjoin,
                                           exclude_colored_coin=exclude_colored_coin)

    np.random.seed(seed)
    coinbase_transactions = np.random.choice(range(start, end), size=num_starts, replace=False)

    # sample the transactions
    transactions = set()
    for block_num in coinbase_transactions:
        new_transactions = sample_from_coinbase_transaction(db=db, block_num=block_num, forbidden=forbidden,
                                                            max_transactions_depth=max_transactions_depth,
                                                            max_depth=max_depth, end=end, seed=seed)
        transactions = transactions.union(new_transactions)
    transactions = [Transaction(block_num=b, position=p) for b, p in transactions]

    # insert the sampled transactions
    hc_ds = DataService(**hc_db)
    hc_ds.insert(table=table_name, objs=transactions)
