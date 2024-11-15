
from blockchain.models.transaction import TransactionFeatures


def is_big(features: TransactionFeatures, max_number_edges: int = 50):
    if features.num_input_ids == 1:
        return False
    elif features.num_output_ids == 1:
        return False
    elif (features.num_input_ids * features.num_output_ids) < max_number_edges:
        return False
    return True
