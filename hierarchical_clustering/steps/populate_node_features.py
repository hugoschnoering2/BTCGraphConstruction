
from tqdm import tqdm

import psycopg2
from mpire import WorkerPool

from database.utils import DataService

from hierarchical_clustering.dbmodels.edge import UpTransactionEdge, DownTransactionEdge
from hierarchical_clustering.dbmodels.node_features import UpNodeFeatures, DownNodeFeatures


def populate_features(hc_db: dict, node_model, edge_model, start: int, end: int, block_step: int):

    hc_ds = DataService(**hc_db)

    # if the table already exists skip this step
    table_name = node_model.table_name(start=start, end=end)

    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        row = hc_ds.execute_query(query=query, fetch="one")
        if row["count"] > 0:
            return
    except psycopg2.errors.UndefinedTable:
        pass

    hc_ds.execute_query(query=node_model.create_table(start, end))
    hc_ds.execute_query(query=edge_model.create_index_reveal(start, end))

    num_steps = (end - start) // block_step + 1

    # feature related to incoming edges
    dict_degree_in = dict()
    dict_total_transactions_in = dict()
    dict_first_transaction_in = dict()
    dict_last_transaction_in = dict()
    dict_min_received = dict()
    dict_max_received = dict()
    dict_total_received = dict()

    # features related to outgoing edges
    dict_degree_out = dict()
    dict_total_transactions_out = dict()
    dict_first_transaction_out = dict()
    dict_last_transaction_out = dict()
    dict_min_sent = dict()
    dict_max_sent = dict()
    dict_total_sent = dict()

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**hc_db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num) -> tuple[list, list]:
        connector = worker_state["pool"].getconn()
        query = f"SELECT * from {edge_model.table_name(start, end)} where reveal = {block_num}"
        edges = DataService.execute_query_w_connector(connector=connector, query=query, fetch="all")
        in_edges = [(row["b"], row["reveal"], row["last_seen"], row["total"],
                     row["min_sent"], row["max_sent"], row["total_sent"]) for row in edges]
        out_edges = [(row["a"], row["reveal"], row["last_seen"], row["total"],
                      row["min_sent"], row["max_sent"], row["total_sent"]) for row in edges]
        worker_state["pool"].putconn(connector)
        return in_edges, out_edges

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        if len(block_nums) == 0:
            continue

        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_edges = pool.map(get_edges_block, block_nums, progress_bar=False,
                                   worker_init=init_db_conn, worker_exit=close_db_conn)

        for edges in block_edges:

            in_edges, out_edges = edges

            for node_id, first_seen, last_seen, total_transactions, min_received, max_received, total_received \
                    in in_edges:

                dict_degree_in[node_id] = dict_degree_in.get(node_id, 0) + 1
                dict_total_transactions_in[node_id] = dict_total_transactions_in.get(node_id, 0) + total_transactions
                dict_first_transaction_in[node_id] = min(dict_first_transaction_in.get(node_id, 10000000), first_seen)
                dict_last_transaction_in[node_id] = max(dict_last_transaction_in.get(node_id, -1), last_seen)
                dict_min_received[node_id] = min(dict_min_received.get(node_id, 100000000000000000000), min_received)
                dict_max_received[node_id] = max(dict_max_received.get(node_id, -1), max_received)
                dict_total_received[node_id] = dict_total_received.get(node_id, 0) + total_received

            for node_id, first_seen, last_seen, total_transactions, min_sent, max_sent, total_sent in out_edges:
                dict_degree_out[node_id] = dict_degree_out.get(node_id, 0) + 1
                dict_total_transactions_out[node_id] = dict_total_transactions_out.get(node_id, 0) + total_transactions
                dict_first_transaction_out[node_id] = min(dict_first_transaction_out.get(node_id, 10000000), first_seen)
                dict_last_transaction_out[node_id] = max(dict_last_transaction_out.get(node_id, -1), last_seen)
                dict_min_sent[node_id] = min(dict_min_sent.get(node_id, 100000000000000000000), min_sent)
                dict_max_sent[node_id] = max(dict_max_sent.get(node_id, -1), max_sent)
                dict_total_sent[node_id] = dict_total_sent.get(node_id, 0) + total_sent

    new_objs = []
    hc_ds = DataService(**hc_db)

    all_node_ids = set(list(dict_degree_in.keys())).union(list(dict_degree_out.keys()))

    try:

        for ind, node_id in enumerate(all_node_ids):

            new_objs.append(node_model(node_id=node_id,
                                       degree_in=dict_degree_in.get(node_id),
                                       total_transactions_in=dict_total_transactions_in.get(node_id),
                                       first_transaction_in=dict_first_transaction_in.get(node_id),
                                       last_transaction_in=dict_last_transaction_in.get(node_id),
                                       min_received=dict_min_received.get(node_id),
                                       max_received=dict_max_received.get(node_id),
                                       total_received=dict_total_received.get(node_id),
                                       degree_out=dict_degree_out.get(node_id),
                                       total_transactions_out=dict_total_transactions_out.get(node_id),
                                       first_transaction_out=dict_first_transaction_out.get(node_id),
                                       last_transaction_out=dict_last_transaction_out.get(node_id),
                                       min_sent=dict_min_sent.get(node_id),
                                       max_sent=dict_max_sent.get(node_id),
                                       total_sent=dict_total_sent.get(node_id)
                                       )
                            )

            if ind % 500000 == 0:
                hc_ds.insert(table=node_model.table_name(start, end), objs=new_objs)
                new_objs = []

        hc_ds.insert(table=node_model.table_name(start, end), objs=new_objs)

    except Exception as e:
        print(e)
        hc_ds.execute_query(query=node_model.drop_table(start, end))
        raise e


def populate_node_features(hc_db: dict, start: int, end: int, block_step: int):

    # features for the up-node
    populate_features(hc_db=hc_db, node_model=UpNodeFeatures, edge_model=UpTransactionEdge,
                      start=start, end=end, block_step=block_step)

    # features for the down-node
    populate_features(hc_db=hc_db, node_model=DownNodeFeatures, edge_model=DownTransactionEdge,
                      start=start, end=end, block_step=block_step)
