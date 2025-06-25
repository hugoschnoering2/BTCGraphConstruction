
import numpy as np
from tqdm import tqdm
from mpire import WorkerPool

import psycopg2
from database.dbmodels.node import Node
from database.dbmodels.txo import Created_TXO, Spent_TXO

from hierarchical_clustering.dbmodels.alias import Alias
from hierarchical_clustering.dbmodels.transactions import Transaction
from hierarchical_clustering.dbmodels.edge import UpTransactionEdge, DownTransactionEdge

from database.dataService import DataService
from database.utils import query_input_txos, query_output_txos

from blockchain.models.transaction import BatchBlockTransactions


def populate_edges(db: dict, hc_db: dict, start: int, end: int, block_step: int = 1):

    hc_ds = DataService(**hc_db)

    # if the table already exists skip this step
    table_name = UpTransactionEdge.table_name(start=start, end=end)

    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        row = hc_ds.execute_query(query=query, fetch="one")
        if row["count"] > 0:
            return
    except psycopg2.errors.UndefinedTable:
        pass

    # create the table - up = alias-level
    hc_ds.execute_query(query=UpTransactionEdge.create_table(start, end))
    hc_ds.execute_query(query=UpTransactionEdge.create_constraint_a_b(start, end))

    # create the table - down = node-level
    hc_ds.execute_query(query=DownTransactionEdge.create_table(start, end))
    hc_ds.execute_query(query=DownTransactionEdge.create_constraint_a_b(start, end))

    # create all indexes to optimize the execution speed
    ds = DataService(**db)
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Spent_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_id())

    # get the sampled transactions
    hc_ds = DataService(**hc_db)
    query = f"SELECT * FROM {Transaction.table_name(start, end)}"
    transactions = hc_ds.execute_query(query=query, fetch="all")

    # get the positions to be collected for each block
    block2positions = {}
    for transaction in transactions:
        block2positions[transaction["block_num"]] = (block2positions.get(transaction["block_num"], [])
                                                     + [transaction["position"]])

    # get the alias
    query = f"SELECT * FROM {Alias.table_name(start, end)}"
    alias = hc_ds.execute_query(query=query, fetch="all")
    node2alias = {row["node_id"]: row["alias"] for row in alias}

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)
        worker_state["block2positions"] = block2positions
        worker_state["node2alias"] = node2alias

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num):

        connector = worker_state["pool"].getconn()
        positions = worker_state["block2positions"][block_num]
        all_alias = worker_state["node2alias"]

        query_inputs = query_input_txos(block_num=block_num, join_node=True, join_alias=False,
                                        exclude_coinjoin=False, exclude_colored_coin=False,
                                        only_positions=positions)
        input_txos = DataService.execute_query_w_connector(connector=connector, query=query_inputs, fetch="all")
        input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                       "node_id": row["node_id"], "value": bytes(row["value"]),
                       "alias": all_alias.get(row["node_id"], row["node_id"]),
                       "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in input_txos]

        query_outputs = query_output_txos(block_num=block_num, join_node=True, join_alias=False,
                                          exclude_coinjoin=False, exclude_colored_coin=False,
                                          only_positions=positions)
        output_txos = DataService.execute_query_w_connector(connector=connector, query=query_outputs, fetch="all")
        output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                        "node_id": row["node_id"], "value": bytes(row["value"]),
                        "alias": all_alias.get(row["node_id"], row["node_id"]),
                        "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in output_txos]
        worker_state["pool"].putconn(connector)

        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num,
                                                              input_txos=input_txos,
                                                              output_txos=output_txos)

        new_up_edges_, new_down_edges_ = [], []

        for transaction in batch_transactions.transactions.values():

            # compute the value received by each alias
            value_received_node = dict()
            value_received_alias = dict()
            for in_txo in transaction.input_txos:
                value_received_node[in_txo.node_id] = value_received_node.get(in_txo.node_id, 0) - in_txo.value_int
                value_received_alias[in_txo.alias] = value_received_alias.get(in_txo.alias, 0) - in_txo.value_int
            for out_txo in transaction.output_txos:
                value_received_node[out_txo.node_id] = value_received_node.get(out_txo.node_id, 0) + out_txo.value_int
                value_received_alias[out_txo.alias] = value_received_alias.get(out_txo.alias, 0) + out_txo.value_int

            # total value sent in the transaction
            total_value_sent_alias = - np.sum([v for v in value_received_alias.values() if v < 0])
            total_value_sent_node = - np.sum([v for v in value_received_node.values() if v < 0])

            # add the new down-edges
            for sender, amount_sent in value_received_node.items():
                if amount_sent >= 0:
                    continue
                for recipient, amount_received in value_received_node.items():
                    if amount_received <= 0:
                        continue
                    new_down_edges_.append((sender, recipient,
                                            int(- amount_sent / total_value_sent_node * amount_received)))

            # add the new down-edges
            for sender, amount_sent in value_received_alias.items():
                if amount_sent >= 0:
                    continue
                for recipient, amount_received in value_received_alias.items():
                    if amount_received <= 0:
                        continue
                    new_up_edges_.append((sender, recipient,
                                          int(- amount_sent / total_value_sent_alias * amount_received)))

        return new_up_edges_, new_down_edges_

    block_nums = sorted(list(block2positions.keys()))
    num_steps = len(block_nums) // block_step + 1

    try:
        for i in tqdm(range(num_steps)):

            block_indexes = range(i * block_step, min((i + 1) * block_step, len(block_nums) - 1))
            step_block_nums = [block_nums[idx] for idx in block_indexes]
            if len(step_block_nums) == 0:
                continue

            # we collect the block transactions from the selected blocks
            with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
                block_edges = pool.map(get_edges_block, step_block_nums, progress_bar=False,
                                       worker_init=init_db_conn, worker_exit=close_db_conn)

            new_up_edges, new_down_edges = dict(), dict()

            for block_num, edges in zip(step_block_nums, block_edges):

                up_edges, down_edges = edges

                for sender_alias, recipient_alias, value in up_edges:
                    if (sender_alias, recipient_alias) in new_up_edges:
                        transaction_edge = new_up_edges[(sender_alias, recipient_alias)]
                        transaction_edge.total += 1
                        transaction_edge.reveal = min(transaction_edge.reveal, block_num)
                        transaction_edge.last_seen = max(transaction_edge.last_seen, block_num)
                        transaction_edge.min_sent = min(transaction_edge.min_sent, value)
                        transaction_edge.max_sent = max(transaction_edge.max_sent, value)
                        transaction_edge.total_sent += value
                    else:
                        new_up_edges[(sender_alias, recipient_alias)] = UpTransactionEdge(
                            a=sender_alias, b=recipient_alias,
                            reveal=block_num, last_seen=block_num, total=1,
                            min_sent=value, max_sent=value,  total_sent=value)

                for sender_node, recipient_node, value in down_edges:
                    if (sender_node, recipient_node) in new_down_edges:
                        transaction_edge = new_down_edges[(sender_node, recipient_node)]
                        transaction_edge.total += 1
                        transaction_edge.reveal = min(transaction_edge.reveal, block_num)
                        transaction_edge.last_seen = max(transaction_edge.last_seen, block_num)
                        transaction_edge.min_sent = min(transaction_edge.min_sent, value)
                        transaction_edge.max_sent = max(transaction_edge.max_sent, value)
                        transaction_edge.total_sent += value
                    else:
                        new_down_edges[(sender_node, recipient_node)] = DownTransactionEdge(
                            a=sender_node, b=recipient_node,
                            reveal=block_num, last_seen=block_num, total=1,
                            min_sent=value, max_sent=value,  total_sent=value)

            on_conflict_do = (" ON CONFLICT (a,b) DO UPDATE SET "
                              "reveal = LEAST(t.reveal,EXCLUDED.reveal), "
                              "last_seen = GREATEST(t.last_seen,EXCLUDED.last_seen), "
                              "total = t.total + EXCLUDED.total, "
                              "min_sent = LEAST(t.min_sent,EXCLUDED.min_sent), "
                              "max_sent = GREATEST(t.max_sent, EXCLUDED.max_sent), "
                              "total_sent = t.total_sent + EXCLUDED.total_sent")

            new_up_edges = list(new_up_edges.values())
            DataService(**hc_db).insert(table=UpTransactionEdge.table_name(start, end), objs=new_up_edges,
                                        on_conflict_do=on_conflict_do)

            new_down_edges = list(new_down_edges.values())
            DataService(**hc_db).insert(table=DownTransactionEdge.table_name(start, end), objs=new_down_edges,
                                        on_conflict_do=on_conflict_do)

        DataService(**hc_db).execute_query(UpTransactionEdge.drop_constraint_a_b(start, end))
        DataService(**hc_db).execute_query(DownTransactionEdge.drop_constraint_a_b(start, end))

    except Exception as e:
        print(e)
        hc_ds = DataService(**hc_db)
        hc_ds.execute_query(UpTransactionEdge.drop_table(start, end))
        hc_ds.execute_query(DownTransactionEdge.drop_table(start, end))
        raise e
