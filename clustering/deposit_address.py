
from blockchain.models.transaction import Transaction


def deposit_address_heuristic(transaction: Transaction, min_num_input_ids: int) -> list:

    if ((len(list({txo.node_id for txo in transaction.input_txos})) < min_num_input_ids) or
            (len(list({txo.node_id for txo in transaction.output_txos})) > 1)):
        return []
    else:
        return list({txo.node_id for txo in transaction.input_txos})
