
import numpy as np
from tqdm import tqdm
from mpire import WorkerPool

from database.dbmodels.alias import Alias
from database.dbmodels.node import Node
from database.dbmodels.edge import ClusterTransactionEdge, TransactionEdge
from database.dbmodels.txo import Created_TXO, Spent_TXO
from database.dbmodels.coinjoin import CoinJoin
from database.dbmodels.colored_coin import ColoredCoin
from database.dataService import DataService


def prepare_tables(db: dict):

    ds = DataService(**db)

    # delete indexes
    ds.execute_query(Alias.drop_index_node_id())

    ds.execute_query(ClusterTransactionEdge.drop_index_a())
    ds.execute_query(ClusterTransactionEdge.drop_index_alias())

    ds.execute_query(Node.drop_index_node_id())
    ds.execute_query(Node.drop_index_reveal())

    ds.execute_query(TransactionEdge.drop_index_reveal())
    ds.execute_query(TransactionEdge.drop_index_a())
    ds.execute_query(TransactionEdge.drop_index_b())

    # create a new index
    ds.execute_query(Spent_TXO.create_index_id())


def get_forbidden_transactions(db: dict, start: int, end: int, exclude_coinjoin: bool = True,
                               exclude_colored_coin: bool = True) -> list:

    forbidden = []

    ds = DataService(**db)

    if exclude_coinjoin:
        query = f"SELECT block,position FROM {CoinJoin.table_name()} WHERE block >= {start}"
        if end is not None:
            query += f" AND block <= {end}"
        rows = ds.execute_query(query, fetch="all")
        forbidden.extend([(int(row["block"]), int(row["position"])) for row in rows])

    if exclude_colored_coin:
        query = f"SELECT block,position FROM {ColoredCoin.table_name()} WHERE block >= {start}"
        if end is not None:
            query += f" AND block <= {end}"
        rows = ds.execute_query(query, fetch="all")
        forbidden.extend([(int(row["block"]), int(row["position"])) for row in rows])

    return forbidden


def get_new_transactions(worker_state, block_num, position) -> list:

    connector = worker_state["pool"].getconn()

    # get the transactions that have created the TXO spent in the current transaction
    query = f"SELECT txo_id FROM {Created_TXO.table_name()} WHERE block_num = {block_num} AND position = {position}"
    query = (f"SELECT spent.block_num,spent.position FROM ({query}) AS created "
             f"INNER JOIN {Spent_TXO.table_name()} AS spent ON spent.txo_id = created.txo_id")
    rows = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")

    worker_state["pool"].putconn(connector)

    return [(row["block_num"], row["position"]) for row in rows]


def sample_from_coinbase_transaction(db: dict, block_num: int, max_depth: int = None,
                                     max_transactions_depth: int = None,
                                     end: int = None, forbidden: list = None, seed: int = 1):
    """Implement a BFS search"""

    if forbidden is None:
        forbidden = []

    if end is not None:
        assert block_num <= end

    # set the seed for reproducibility
    np.random.seed(seed)

    # initialisation
    visited_transactions: set[tuple[int, int]] = set()
    transactions = {(block_num, 0)}

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    # iterate while the transactions is not empty
    depth = 0
    while len(transactions) > 0 and (max_depth is None or depth <= max_depth):

        visited_transactions = visited_transactions.union(transactions)

        new_transactions = []
        with WorkerPool(n_jobs=12, start_method="threading", use_worker_state=True) as pool:
            batch_new_transactions = pool.map(get_new_transactions, transactions,
                                              worker_init=init_db_conn, worker_exit=close_db_conn)
        for e in batch_new_transactions:
            new_transactions.extend(e)

        transactions = list(set([(b, p) for (b, p) in new_transactions
                                 if (b, p) not in visited_transactions and (b, p) not in forbidden
                                 and (end is None or b <= end)]))

        if max_transactions_depth is not None and max_transactions_depth < len(transactions):
            transactions_idxs = np.random.choice(range(len(transactions)), size=max_transactions_depth, replace=False)
            transactions = [transactions[idx] for idx in transactions_idxs]

        depth += 1

    visited_transactions = visited_transactions.union(transactions)

    return visited_transactions
