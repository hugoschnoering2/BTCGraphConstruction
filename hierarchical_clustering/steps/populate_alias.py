
import numpy as np
from tqdm import tqdm
from mpire import WorkerPool

import psycopg2
from database.utils import DataService

from database.utils import query_input_txos, query_output_txos
from blockchain.models.transaction import BatchBlockTransactions

from clustering.get_data import get_exchange_rate_block
from clustering.common_input_ownership import common_input_ownership_heuristic
from clustering.deposit_address import deposit_address_heuristic
from clustering.change import change_address_heuristic, round_output_value_heuristic
from clustering.force_merge_input import force_merge_input_heuristic
from clustering.unionFind import UnionFind

from hierarchical_clustering.dbmodels.transactions import Transaction
from hierarchical_clustering.dbmodels.alias import Alias


def populate_alias(db: dict,
                   start: int,
                   end: int,
                   hc_db: dict,
                   block_step: int,
                   common_input_ownership: bool = False,
                   deposit_address: bool = False,
                   deposit_address_min_num_inputs: int = 25,
                   change_address: bool = False,
                   round_output_value: bool = False,
                   round_output_value_x: float = 0.1,
                   round_output_value_j: int = 2,
                   force_merge_inputs: bool = False,
                   ):

    # if the table already exists skip this step
    table_name = Alias.table_name(start=start, end=end)
    hc_ds = DataService(**hc_db)
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        row = hc_ds.execute_query(query=query, fetch="one")
        if row["count"] > 0:
            return
    except psycopg2.errors.UndefinedTable:
        pass

    # create the table if it does not exist
    hc_ds.execute_query(Alias.create_table(start, end))

    # get the sampled transactions
    hc_ds = DataService(**hc_db)
    query = f"SELECT * FROM {Transaction.table_name(start, end)}"
    transactions = hc_ds.execute_query(query=query, fetch="all")

    # get the positions to be collected for each block
    block2positions = {}
    for transaction in transactions:
        block2positions[transaction["block_num"]] = (block2positions.get(transaction["block_num"], [])
                                                     + [transaction["position"]])

    # get the bitcoin exchange rate
    exchange_rate_block = get_exchange_rate_block()

    join_node = change_address or force_merge_inputs or round_output_value

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)
        worker_state["block2positions"] = block2positions

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_clusters_block(worker_state, block_num: int) -> list:

        connector = worker_state["pool"].getconn()
        positions = worker_state["block2positions"][block_num]

        query_inputs = query_input_txos(block_num=block_num, join_node=join_node, exclude_coinjoin=False,
                                        exclude_colored_coin=False, only_positions=positions)
        input_txos = DataService.execute_query_w_connector(connector=connector, query=query_inputs, fetch="all")
        input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                       "node_id": row["node_id"], "value": bytes(row["value"]),
                       "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in input_txos]

        query_outputs = query_output_txos(block_num=block_num, join_node=join_node, exclude_coinjoin=False,
                                          exclude_colored_coin=False, only_positions=positions)
        output_txos = DataService.execute_query_w_connector(connector=connector, query=query_outputs, fetch="all")
        output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                        "node_id": row["node_id"], "value": bytes(row["value"]),
                        "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in output_txos]
        worker_state["pool"].putconn(connector)

        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num,
                                                              input_txos=input_txos,
                                                              output_txos=output_txos)

        price_blk = exchange_rate_block.get(block_num)

        clusters = []

        for position, transaction in batch_transactions.transactions.items():
            if common_input_ownership:
                new_cluster = common_input_ownership_heuristic(transaction=transaction)
                if len(new_cluster) > 1:
                    clusters.append(new_cluster)
            if deposit_address:
                new_cluster = deposit_address_heuristic(transaction=transaction,
                                                        min_num_input_ids=deposit_address_min_num_inputs)
                if len(new_cluster) > 1:
                    clusters.append(new_cluster)
            if change_address:
                new_cluster = change_address_heuristic(transaction=transaction, max_block=end)
                if len(new_cluster) > 1:
                    clusters.append(new_cluster)
            if round_output_value:
                if price_blk is not None:
                    price_sat_blk = float(price_blk) / 10 ** 8
                    round_output_value_i = np.floor(np.log(round_output_value_x / price_sat_blk) / np.log(10))
                    new_cluster = round_output_value_heuristic(transaction=transaction, max_block=end,
                                                               i=int(round_output_value_i), j=int(round_output_value_j))
                    if len(new_cluster) > 1:
                        clusters.append(new_cluster)
            if force_merge_inputs:
                new_cluster = force_merge_input_heuristic(transaction=transaction, max_block=end)
                if len(new_cluster) > 1:
                    clusters.append(new_cluster)

        return clusters

    block_nums = sorted(list(block2positions.keys()))
    num_steps = len(block_nums) // block_step + 1

    uf = UnionFind()

    for i in tqdm(range(num_steps)):

        block_indexes = range(i * block_step, min((i + 1) * block_step, len(block_nums) - 1))
        step_block_nums = [block_nums[idx] for idx in block_indexes]
        if len(step_block_nums) == 0:
            continue

        # we collect the block transactions from the selected blocks
        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_clusters = pool.map(get_clusters_block, step_block_nums, progress_bar=False,
                                      worker_init=init_db_conn, worker_exit=close_db_conn)

        for cluster_batch in block_clusters:
            for cluster in cluster_batch:
                uf.add(new_ids=cluster)
                first_node = cluster[0]
                for node in cluster[1:]:
                    uf.union(first_node, node)

    try:
        new_objs = []
        hc_ds = DataService(**hc_db)
        for ind, node_id in enumerate(uf.cluster_root.keys()):
            root = uf.find(node_id)
            new_objs.append(Alias(node_id=node_id, alias=root))
            if ind % 500000 == 0:
                hc_ds.insert(table=Alias.table_name(start, end), objs=new_objs)
                new_objs = []
        hc_ds.insert(table=Alias.table_name(start, end), objs=new_objs)
    except Exception as e:
        print(e)
        hc_ds = DataService(**hc_db)
        hc_ds.execute_query(Alias.drop_table(start, end))
        raise e
