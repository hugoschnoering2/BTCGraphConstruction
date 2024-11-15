
from blockchain.models.transaction import Transaction


def change_address_heuristic(transaction: Transaction, max_block: int) -> list:

    if len(transaction.input_txos) != 1:  # if more than one input
        return []

    if len(transaction.output_txos) != 2:
        return []

    features = transaction.compute_features()
    if features.num_output_ids != 2:
        return []
    if len(set(features.input_ids).intersection(set(features.output_ids))) != 0:
        return []

    input_txo = transaction.input_txos[0]
    if input_txo.reuse and (input_txo.reuse <= max_block):
        return []

    first_output_txo = transaction.output_txos[0]
    second_output_txo = transaction.output_txos[1]
    if first_output_txo.reuse and (first_output_txo.reuse <= max_block):
        is_first_output_reused = True
    else:
        is_first_output_reused = False
    if second_output_txo.reuse and (second_output_txo.reuse <= max_block):
        is_second_output_reused = True
    else:
        is_second_output_reused = False

    if is_first_output_reused and not is_second_output_reused:
        return [input_txo.node_id, second_output_txo.node_id]
    elif not is_first_output_reused and is_second_output_reused:
        return [input_txo.node_id, first_output_txo.node_id]
    else:
        return []


def round_output_value_heuristic(transaction: Transaction, max_block: int, i: int, j: int):

    if not (0 <= j < i):
        return []

    if len(transaction.input_txos) != 1:  # if more than one input
        return []

    if len(transaction.output_txos) != 2:
        return []

    # one output address equals the input address
    features = transaction.compute_features()
    if features.num_output_ids != 2:
        return []
    if len(set(features.input_ids).intersection(set(features.output_ids))) != 0:
        return []

    # input address reused
    input_txo = transaction.input_txos[0]
    if input_txo.reuse and (input_txo.reuse <= max_block):
        return []

    first_output_txo = transaction.output_txos[0]
    second_output_txo = transaction.output_txos[1]

    if (not (first_output_txo.reuse and (first_output_txo.reuse <= max_block))
            and str(second_output_txo.value_int).endswith("0" * i)
            and not str(first_output_txo.value_int).endswith("0" * (i - j))):
        return [input_txo.node_id, first_output_txo.node_id]

    if (not (second_output_txo.reuse and (second_output_txo.reuse <= max_block))
            and str(first_output_txo.value_int).endswith("0" * i)
            and not str(second_output_txo.value_int).endswith("0" * (i - j))):
        return [input_txo.node_id, second_output_txo.node_id]

    return []
