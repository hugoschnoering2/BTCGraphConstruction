
import pandas as pd
from tqdm import tqdm

from mpire import WorkerPool

from database.dataService import DataService
from database.utils import query_input_txos, query_output_txos

from database.dbmodels.alias import Alias
from database.dbmodels.node import Node
from database.dbmodels.txo import Created_TXO, Spent_TXO
from database.dbmodels.coinjoin import CoinJoin
from database.dbmodels.colored_coin import ColoredCoin
from database.dbmodels.edge import ClusterTransactionEdge

from blockchain.models.transaction import BatchBlockTransactions


def add_cluster_transaction_edges(db: dict, start: int, end: int, num_steps: int = 10000):

    ds = DataService(**db)

    # create all indexes
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Spent_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_id())
    ds.execute_query(query=Alias.create_index_node_id())
    ds.execute_query(query=CoinJoin.create_index_block())
    ds.execute_query(query=ColoredCoin.create_index_block())

    # drop and create the table
    ds.execute_query(query=ClusterTransactionEdge.drop_table())
    ds.execute_query(query=ClusterTransactionEdge.create_table())

    query = (f"SELECT block_num, COUNT(*) FROM {Spent_TXO.table_name()} "
             f"WHERE block_num >= {start} AND block_num <= {end} GROUP BY block_num")
    spent_by_block_num = ds.execute_query(query=query, fetch="all")
    spent_by_block_num = pd.DataFrame(spent_by_block_num).set_index("block_num")

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_cluster_edges_block(worker_state, block_num):
        connector = worker_state["pool"].getconn()
        query_inputs = query_input_txos(block_num=block_num, join_node=False, join_alias=True, exclude_coinjoin=True,
                                        exclude_colored_coin=True, only_one_per_position=False)
        input_txos = DataService.execute_query_w_connector(connector=connector, query=query_inputs, fetch="all")
        input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                       "node_id": row["node_id"], "value": bytes(row["value"]), "alias": row["alias"],
                       "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in input_txos]
        query_outputs = query_output_txos(block_num=block_num, join_node=False, join_alias=True,
                                          exclude_coinjoin=True, exclude_colored_coin=True)
        output_txos = DataService.execute_query_w_connector(connector=connector, query=query_outputs, fetch="all")
        output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                        "node_id": row["node_id"], "value": bytes(row["value"]), "alias": row["alias"],
                        "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in output_txos]
        worker_state["pool"].putconn(connector)
        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num, input_txos=input_txos,
                                                              output_txos=output_txos)
        edges = set()
        for transaction in batch_transactions.transactions.values():
            if len(transaction.input_txos) == 0:
                continue
            input_alias = transaction.input_txos[0].alias
            input_node_ids = set([txo.node_id for txo in transaction.input_txos if txo.alias == input_alias])
            output_node_ids = set([txo.node_id for txo in transaction.output_txos if txo.alias == input_alias]) - input_node_ids
            transaction_edges = set([(input_alias, input_id, output_id) for input_id in input_node_ids
                                     for output_id in output_node_ids
                                     if input_id != output_id])
            edges = edges.union(transaction_edges)
        return edges

    total_spent = spent_by_block_num.sum()["count"]
    max_spent_per_step = int(total_spent / num_steps)

    i = 0

    with tqdm(total=total_spent) as pbar:

        while (i < len(spent_by_block_num) and spent_by_block_num.index[i] <= end):

            total_spent_txos = 0
            block_nums = []

            while i < len(spent_by_block_num) \
                    and (spent_by_block_num.index[i] <= end) \
                    and ((len(block_nums) == 0) or (total_spent_txos + spent_by_block_num.iloc[i, 0]
                                                    < max_spent_per_step)):

                block_nums.append(spent_by_block_num.index[i])
                total_spent_txos += spent_by_block_num.iloc[i, 0]
                i += 1

            with WorkerPool(n_jobs=12, start_method="threading", use_worker_state=True) as pool:
                block_edges = pool.map(get_cluster_edges_block, block_nums, progress_bar=False,
                                       worker_init=init_db_conn, worker_exit=close_db_conn)

            batch_block_edges = set()
            for edges in block_edges:
                batch_block_edges = batch_block_edges.union(edges)
            batch_block_edges = [ClusterTransactionEdge(alias=alias, a=a, b=b) for alias, a, b in batch_block_edges]

            ds = DataService(**db)
            ds.insert(table=ClusterTransactionEdge.table_name(), objs=batch_block_edges, on_conflict_do_nothing=True)

            pbar.update(total_spent_txos)
