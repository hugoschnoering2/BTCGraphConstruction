
import json
import datetime

import numpy as np
import pandas as pd
from tqdm import tqdm
from mpire import WorkerPool

from database.utils import DataService

from database.utils import prepare_table
from database.dbmodels.alias import Alias
from database.dbmodels.node import Node
from database.dbmodels.txo import Created_TXO, Spent_TXO
from database.dbmodels.coinjoin import CoinJoin
from database.dbmodels.colored_coin import ColoredCoin

from database.utils import query_input_txos, query_output_txos
from blockchain.models.transaction import BatchBlockTransactions
from clustering.common_input_ownership import common_input_ownership_heuristic
from clustering.deposit_address import deposit_address_heuristic
from clustering.change import change_address_heuristic, round_output_value_heuristic
from clustering.force_merge_input import force_merge_input_heuristic
from clustering.unionFind import UnionFind


with open("clustering/data/market-price.json") as f:
    price = json.load(f)["market-price"]
    price = pd.DataFrame(price)
    price.columns = ["date", "price"]
    price["date"] = price["date"].apply(lambda x: datetime.datetime.fromtimestamp(x // 1000))
    price = price.set_index("date")
    price = price.loc[(price.price > 0) & (price.index >= datetime.datetime(2012, 1, 1)), "price"]


with open("clustering/data/block-dates.json") as f:
    blocks = json.load(f)
    blocks = {datetime.datetime.fromtimestamp(v): int(k) for k, v in blocks.items()}
    blocks = pd.Series(blocks, name="index")


block_price = pd.concat([price, blocks], axis=1).ffill().dropna()
block_price = block_price.reset_index(drop=True).groupby("index").mean()
block_price = block_price.reindex(pd.RangeIndex(start=block_price.index.min(), stop=block_price.index.max()+1)).ffill()
block_price = dict(block_price["price"])


def populate_alias(db: dict,
                   start: int,
                   end: int,
                   do: bool,
                   block_step: int,
                   exclude_coinjoin: bool = True,
                   common_input_ownership: bool = False,
                   deposit_address: bool = False,
                   deposit_address_min_num_inputs: int = 25,
                   change_address: bool = False,
                   round_output_value: bool = False,
                   round_output_value_x: float = 0.1,
                   round_output_value_j: int = 2,
                   force_merge_inputs: bool = False,
                   insert_alias: bool = False):

    if not do:
        return None

    if insert_alias:
        print(f"Populating the table {Alias.table_name()}, target block: {end}")
        ds = DataService(**db)
        ds.execute_query(query=Alias.drop_table())
        prepare_table(ds=ds, cls=Alias)

    # creating all necessary indexes
    ds = DataService(**db)
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Spent_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_id())
    ds.execute_query(query=CoinJoin.create_index_block())
    ds.execute_query(query=ColoredCoin.create_index_block())

    join_node = change_address or force_merge_inputs or round_output_value

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_clusters_block(worker_state, block_num) -> list:

        connector = worker_state["pool"].getconn()

        query_inputs = query_input_txos(block_num=block_num, join_node=join_node, exclude_coinjoin=exclude_coinjoin)
        input_txos = DataService.execute_query_w_connector(connector=connector, query=query_inputs, fetch="all")
        input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                       "node_id": row["node_id"], "value": bytes(row["value"]),
                       "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in input_txos]

        query_outputs = query_output_txos(block_num=block_num, join_node=join_node, exclude_coinjoin=exclude_coinjoin)
        output_txos = DataService.execute_query_w_connector(connector=connector, query=query_outputs, fetch="all")
        output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                        "node_id": row["node_id"], "value": bytes(row["value"]),
                        "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in output_txos]
        worker_state["pool"].putconn(connector)

        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num,
                                                              input_txos=input_txos,
                                                              output_txos=output_txos)

        price_blk = block_price.get(block_num)

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

    num_steps = (end - start) // block_step + 1

    uf = UnionFind()

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        if len(block_nums) == 0:
            continue

        # we collect the block transactions from the selected blocks
        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_clusters = pool.map(get_clusters_block, block_nums, progress_bar=False,
                                      worker_init=init_db_conn, worker_exit=close_db_conn)

        for cluster_batch in block_clusters:
            for cluster in cluster_batch:
                uf.add(new_ids=cluster)
                first_node = cluster[0]
                for node in cluster[1:]:
                    uf.union(first_node, node)

    if insert_alias:
        new_objs = []
        ds = DataService(**db)
        for ind, node_id in enumerate(uf.cluster_root.keys()):
            root = uf.find(node_id)
            new_objs.append(Alias(node_id=node_id, alias=root))
            if ind % 500000 == 0:
                ds.insert(table=Alias.table_name(), objs=new_objs)
                new_objs = []
        ds.insert(table=Alias.table_name(), objs=new_objs)
