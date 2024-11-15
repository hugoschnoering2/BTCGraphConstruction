
from tqdm import tqdm

from mpire import WorkerPool

from database.utils import DataService
from database.dbmodels.edge import TransactionEdge
from database.dbmodels.node_features import NodeFeatures


def add_in_transaction_features(db: dict, start: int, end: int, block_step: int):

    num_steps = (end - start) // block_step + 1

    dict_degree_in = dict()
    dict_total_transactions_in = dict()
    dict_first_transaction_in = dict()
    dict_last_transaction_in = dict()
    dict_min_received = dict()
    dict_max_received = dict()
    dict_total_received = dict()

    ds = DataService(**db)
    ds.execute_query(query=NodeFeatures.create_table())
    ds.execute_query(query=TransactionEdge.create_index_reveal())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num) -> list:
        connector = worker_state["pool"].getconn()
        query = f"SELECT * from {TransactionEdge.table_name()} where reveal = {block_num}"
        edges = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        edges = [(row["b"], row["reveal"], row["last_seen"], row["total"],
                  row["min_sent"], row["max_sent"], row["total_sent"]) for row in edges]
        worker_state["pool"].putconn(connector)
        return edges

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        if len(block_nums) == 0:
            continue

        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_edges = pool.map(get_edges_block, block_nums, progress_bar=False,
                                   worker_init=init_db_conn, worker_exit=close_db_conn)

        for edges in block_edges:

            for alias, first_seen, last_seen, total_transactions, min_received, max_received, total_received in edges:

                dict_degree_in[alias] = dict_degree_in.get(alias, 0) + 1
                dict_total_transactions_in[alias] = dict_total_transactions_in.get(alias, 0) + total_transactions
                dict_first_transaction_in[alias] = min(dict_first_transaction_in.get(alias, 10000000), first_seen)
                dict_last_transaction_in[alias] = max(dict_last_transaction_in.get(alias, -1), last_seen)
                dict_min_received[alias] = min(dict_min_received.get(alias, 100000000000000000000), min_received)
                dict_max_received[alias] = max(dict_max_received.get(alias, -1), max_received)
                dict_total_received[alias] = dict_total_received.get(alias, 0) + total_received

    on_conflict_do = (" ON CONFLICT (alias) DO UPDATE SET "
                      "degree_in = EXCLUDED.degree_in, "
                      "total_transactions_in = EXCLUDED.total_transactions_in, "
                      "first_transaction_in = EXCLUDED.first_transaction_in, "
                      "last_transaction_in = EXCLUDED.last_transaction_in, "
                      "min_received = EXCLUDED.min_received, "
                      "max_received = EXCLUDED.max_received, "
                      "total_received = EXCLUDED.total_received")

    new_objs = []
    ds = DataService(**db)

    for ind, alias in enumerate(dict_degree_in.keys()):

        new_objs.append(NodeFeatures(alias=alias,
                                     degree_in=dict_degree_in[alias],
                                     total_transactions_in=dict_total_transactions_in[alias],
                                     first_transaction_in=dict_first_transaction_in[alias],
                                     last_transaction_in=dict_last_transaction_in[alias],
                                     min_received=dict_min_received[alias],
                                     max_received=dict_max_received[alias],
                                     total_received=dict_total_received[alias]))

        if ind % 500000 == 0:
            ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
            new_objs = []

    ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
