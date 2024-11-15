
from blockchain.models.transaction import Transaction


def common_input_ownership_heuristic(transaction: Transaction) -> list:
    return list({txo.node_id for txo in transaction.input_txos})
