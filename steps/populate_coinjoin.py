
import numpy as np

from tqdm import tqdm
from mpire import WorkerPool

from database.dataService import DataService

from database.utils import prepare_table
from database.dbmodels.node import Node
from database.dbmodels.txo import Spent_TXO, Created_TXO
from database.dbmodels.coinjoin import CoinJoin

from blockchain.special_transactions.coinjoin import (is_coinjoin_joinmarket, is_coinjoin_wasabi_1_0,
                                                      is_coinjoin_wasabi_1_1, is_coinjoin_wasabi_2,
                                                      is_coinjoin_samourai, is_tx0_samourai)
from database.utils import query_input_txos, query_output_txos
from blockchain.models.transaction import BatchBlockTransactions

from psycopg2 import InterfaceError


def populate_coinjoins(db: dict, start: int, end: int, do: bool):

    if not do:
        return None

    print(f"Populating the table {CoinJoin.table_name()}, target block: {end}")
    ds = DataService(**db)
    ds.execute_query(query=CoinJoin.drop_table())
    prepare_table(ds=ds, cls=CoinJoin)

    ds = DataService(**db)
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Spent_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_id())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_coinjoins_block(worker_state, block_num):

        success = False

        while not success:

            try:

                connector = worker_state["pool"].getconn()
                query_inputs = query_input_txos(block_num=block_num, join_node=False)
                input_txos = DataService.execute_query_w_connector(connector=connector,
                                                                   query=query_inputs, fetch="all")
                input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                               "node_id": row["node_id"], "value": bytes(row["value"])} for row in input_txos]
                query_outputs = query_output_txos(block_num=block_num, join_node=False)
                output_txos = DataService.execute_query_w_connector(connector=connector,
                                                                    query=query_outputs, fetch="all")
                output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                                "node_id": row["node_id"], "value": bytes(row["value"])} for row in output_txos]
                worker_state["pool"].putconn(connector)

                success = True

            except InterfaceError:
                try:
                    worker_state["pool"].putconn(connector)
                except:
                    pass

        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num,
                                                              input_txos=input_txos,
                                                              output_txos=output_txos)

        new_coinjoins = []
        for transaction in batch_transactions.transactions.values():
            tx_features = transaction.compute_features()
            joinmarket = int(is_coinjoin_joinmarket(tx_features))
            wasabi1 = int(is_coinjoin_wasabi_1_0(tx_features))
            wasabi11 = int(is_coinjoin_wasabi_1_1(tx_features))
            wasabi2 = int(is_coinjoin_wasabi_2(tx_features))
            whirlpool = int(is_coinjoin_samourai(tx_features))
            whirlpool_tx0 = int(is_tx0_samourai(tx_features))
            if np.max([joinmarket, wasabi1, wasabi11, wasabi2, whirlpool, whirlpool_tx0]) > 0:
                new_coinjoins.append(CoinJoin(block=block_num,
                                              position=transaction.position,
                                              joinmarket=joinmarket,
                                              wasabi1=wasabi1,
                                              wasabi11=wasabi11,
                                              wasabi2=wasabi2,
                                              whirlpool=whirlpool,
                                              whirlpool_tx0=whirlpool_tx0))
        if len(new_coinjoins) > 0:
            DataService(**db).insert(table=CoinJoin.table_name(), objs=new_coinjoins, on_conflict_do_nothing=True,
                                     connector=connector)

    block_nums = range(start, end + 1)

    with WorkerPool(n_jobs=10, start_method="threading", use_worker_state=True) as pool:
        pool.map(get_coinjoins_block, tqdm(block_nums), progress_bar=False,
                 worker_init=init_db_conn, worker_exit=close_db_conn)
