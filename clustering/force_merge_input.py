
import numpy as np

from blockchain.models.transaction import Transaction


def force_merge_input_heuristic(transaction: Transaction, max_block: int):

    if len(transaction.input_txos) < 2:
        return []

    if len(transaction.output_txos) != 2:
        return []

    features = transaction.compute_features()

    if features.num_input_ids != features.num_inputs:
        return []
    if features.num_output_ids != 2:
        return []

    payment_output_txo, change_output_txo = transaction.output_txos
    if payment_output_txo.value_int < change_output_txo.value_int:
        payment_output_txo, change_output_txo = change_output_txo, payment_output_txo

    for input_txo in transaction.input_txos:
        if input_txo.reuse and (input_txo.reuse <= max_block):
            return []

    if change_output_txo.reuse and (change_output_txo.reuse <= max_block):
        return []

    if (np.sum([input_txo.value_int for input_txo in transaction.input_txos]) -
            np.min([input_txo.value_int for input_txo in transaction.input_txos])) >= payment_output_txo.value_int:
        return []

    return list(features.input_ids) + [change_output_txo.node_id]
