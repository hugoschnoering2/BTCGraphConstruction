
from tqdm import tqdm

from mpire import WorkerPool

from database.utils import DataService
from database.dbmodels.edge import TransactionEdge
from database.dbmodels.node_features import NodeFeatures


def add_out_transaction_features(db: dict, start: int, end: int, block_step: int):

    num_steps = (end - start) // block_step + 1

    # dict that will store the feature before storing them in the database
    dict_degree_out = dict()
    dict_total_transactions_out = dict()
    dict_first_transaction_out = dict()
    dict_last_transaction_out = dict()
    dict_min_sent = dict()
    dict_max_sent = dict()
    dict_total_sent = dict()

    ds = DataService(**db)
    # create indexes that will accelerate the computation of features
    ds.execute_query(query=NodeFeatures.create_table())
    ds.execute_query(query=TransactionEdge.create_index_reveal())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    # function to get all edges from a specific block
    def get_edges_block(worker_state, block_num) -> list:
        connector = worker_state["pool"].getconn()
        query = f"SELECT * from {TransactionEdge.table_name()} where reveal = {block_num}"
        edges = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        edges = [(row["a"], row["reveal"], row["last_seen"], row["total"],
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

            for alias, first_seen, last_seen, total_transactions, min_sent, max_sent, total_sent in edges:
                dict_degree_out[alias] = dict_degree_out.get(alias, 0) + 1
                dict_total_transactions_out[alias] = dict_total_transactions_out.get(alias, 0) + total_transactions
                dict_first_transaction_out[alias] = min(dict_first_transaction_out.get(alias, 10000000), first_seen)
                dict_last_transaction_out[alias] = max(dict_last_transaction_out.get(alias, -1), last_seen)
                dict_min_sent[alias] = min(dict_min_sent.get(alias, 100000000000000000000), min_sent)
                dict_max_sent[alias] = max(dict_max_sent.get(alias, -1), max_sent)
                dict_total_sent[alias] = dict_total_sent.get(alias, 0) + total_sent

    on_conflict_do = (" ON CONFLICT (alias) DO UPDATE SET "
                      "degree_out = EXCLUDED.degree_out, "
                      "total_transactions_out = EXCLUDED.total_transactions_out, "
                      "first_transaction_out = EXCLUDED.first_transaction_out, "
                      "last_transaction_out = EXCLUDED.last_transaction_out, "
                      "min_sent = EXCLUDED.min_sent, "
                      "max_sent = EXCLUDED.max_sent, "
                      "total_sent = EXCLUDED.total_sent")

    new_objs = []
    ds = DataService(**db)

    for ind, alias in tqdm(enumerate(dict_degree_out.keys()), total=len(dict_degree_out)):

        new_objs.append(NodeFeatures(alias=alias,
                                     degree_out=dict_degree_out[alias],
                                     total_transactions_out=dict_total_transactions_out[alias],
                                     first_transaction_out=dict_first_transaction_out[alias],
                                     last_transaction_out=dict_last_transaction_out[alias],
                                     min_sent=dict_min_sent[alias],
                                     max_sent=dict_max_sent[alias],
                                     total_sent=dict_total_sent[alias]))

        if ind % 500000 == 0:
            ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
            new_objs = []

    ds.insert(table=NodeFeatures.table_name(), objs=new_objs, on_conflict_do=on_conflict_do)
