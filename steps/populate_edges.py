
from tqdm import tqdm
from mpire import WorkerPool

from database.dbmodels.alias import Alias
from database.dbmodels.node import Node
from database.dbmodels.txo import Created_TXO, Spent_TXO
from database.dbmodels.edge import TransactionEdge
from database.dbmodels.coinjoin import CoinJoin
from database.dbmodels.colored_coin import ColoredCoin

from database.dataService import DataService
from database.utils import prepare_table, query_input_txos, query_output_txos

from blockchain.models.transaction import BatchBlockTransactions


def populate_edges(db: dict, start: int, end: int, do: bool, block_step: int, exclude_coinjoin: bool,
                   exclude_colored_coin: bool, only_one_per_position: bool = False):

    assert exclude_coinjoin and only_one_per_position  # Not implemented else

    if not do:
        return None

    ds = DataService(**db)
    start = max(prepare_table(ds=ds, cls=TransactionEdge), start)
    print(f"Populating the table {TransactionEdge.table_name()}, target block: {end} - start: {start}")
    try:
        ds.execute_query(query=TransactionEdge.create_constraint_a_b())
    except Exception as e:
        print(f"Error {type(e)} - {e}")
        print("Impossible to create the constraint on transaction_edges(a,b), press 'y' to continue.")
        assert input() == "y"

    # create all indexes
    ds.execute_query(query=Node.create_index_node_id())
    ds.execute_query(query=Spent_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_block_num())
    ds.execute_query(query=Created_TXO.create_index_id())
    ds.execute_query(query=Alias.create_index_node_id())

    if exclude_coinjoin:  # if we decide to exclude the coinjoin, we need to create an index on the table
        ds.execute_query(query=CoinJoin.create_index_block())

    if exclude_colored_coin:  # if we decide to exclude the coinjoin, we need to create an index on the table
        ds.execute_query(query=ColoredCoin.create_index_block())

    def init_db_conn(worker_state):
        worker_state["pool"] = DataService(**db).pool(min_connection=5, max_connection=20)

    def close_db_conn(worker_state):
        worker_state["pool"].closeall()

    def get_edges_block(worker_state, block_num):

        connector = worker_state["pool"].getconn()

        query_inputs = query_input_txos(block_num=block_num, join_node=False, join_alias=True,
                                        exclude_coinjoin=exclude_coinjoin, exclude_colored_coin=exclude_colored_coin,
                                        only_one_per_position=only_one_per_position)
        input_txos = DataService.execute_query_w_connector(connector=connector, query=query_inputs, fetch="all")
        input_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                       "node_id": row["node_id"], "value": bytes(row["value"]), "alias": row["alias"],
                       "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in input_txos]

        query_outputs = query_output_txos(block_num=block_num, join_node=False, join_alias=True,
                                          exclude_coinjoin=exclude_coinjoin, exclude_colored_coin=exclude_colored_coin)
        output_txos = DataService.execute_query_w_connector(connector=connector, query=query_outputs, fetch="all")
        output_txos = [{"position": row["position"], "txo_id": bytes(row["txo_id"]),
                        "node_id": row["node_id"], "value": bytes(row["value"]), "alias": row["alias"],
                        "reuse": row.get("reuse", -1), "reveal": row.get("reveal", -1)} for row in output_txos]
        worker_state["pool"].putconn(connector)

        batch_transactions = BatchBlockTransactions.from_rows(block_num=block_num,
                                                              input_txos=input_txos,
                                                              output_txos=output_txos)

        new_edges_ = []

        for transaction in batch_transactions.transactions.values():

            if len(transaction.input_txos) != 1:
                continue

            sender_alias = transaction.input_txos[0].alias  # there is only one sender since we have used the cio
            recipients = {txo.alias: 0 for txo in transaction.output_txos if txo.alias != sender_alias}

            for txo in transaction.output_txos:
                recipient_alias = txo.alias
                if recipient_alias == sender_alias:
                    continue
                recipients[recipient_alias] += txo.value_int

            for recipient_alias, value in recipients.items():
                if value > 0:
                    new_edges_.append((sender_alias, recipient_alias, value))

        return new_edges_

    num_steps = (end - start) // block_step + 1

    for i in tqdm(range(num_steps)):

        block_nums = range(start + 1 + i * block_step, start + (i + 1) * block_step + 1)
        block_nums = [n for n in block_nums if (n <= end) and (n > start)]

        # we collect the block transactions from the selected blocks
        with WorkerPool(n_jobs=6, start_method="threading", use_worker_state=True) as pool:
            block_edges = pool.map(get_edges_block, block_nums, progress_bar=False,
                                   worker_init=init_db_conn, worker_exit=close_db_conn)

        new_edges = dict()
        for block_num, edges in zip(block_nums, block_edges):
            for sender_alias, recipient_alias, value in edges:
                if (sender_alias, recipient_alias) in new_edges:
                    transaction_edge = new_edges[(sender_alias, recipient_alias)]
                    transaction_edge.total += 1
                    transaction_edge.reveal = min(transaction_edge.reveal, block_num)
                    transaction_edge.last_seen = max(transaction_edge.last_seen, block_num)
                    transaction_edge.min_sent = min(transaction_edge.min_sent, value)
                    transaction_edge.max_sent = max(transaction_edge.max_sent, value)
                    transaction_edge.total_sent += value
                else:
                    new_edges[(sender_alias, recipient_alias)] = TransactionEdge(
                        a=sender_alias, b=recipient_alias,
                        reveal=block_num, last_seen=block_num, total=1,
                        min_sent=value, max_sent=value,  total_sent=value)

        on_conflict_do = (" ON CONFLICT (a,b) DO UPDATE SET "
                          "reveal = LEAST(t.reveal,EXCLUDED.reveal), "
                          "last_seen = GREATEST(t.last_seen,EXCLUDED.last_seen), "
                          "total = t.total + EXCLUDED.total, "
                          "min_sent = LEAST(t.min_sent,EXCLUDED.min_sent), "
                          "max_sent = GREATEST(t.max_sent, EXCLUDED.max_sent), "
                          "total_sent = t.total_sent + EXCLUDED.total_sent")

        new_edges = list(new_edges.values())
        DataService(**db).insert(table=TransactionEdge.table_name(), objs=new_edges, on_conflict_do=on_conflict_do)

    DataService(**db).execute_query(TransactionEdge.drop_constraint_a_b())
