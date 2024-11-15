
import numpy as np

from blockchain.models.transaction import TransactionFeatures


def is_coinjoin_joinmarket(features: TransactionFeatures, min_num_participants: int = 3):
    anonymity_set = features.occ_most_represented_output_value
    if (anonymity_set is None) or (anonymity_set < min_num_participants):
        return False
    if anonymity_set < features.num_outputs / 2:
        return False
    if anonymity_set > features.num_input_ids:
        return False
    if features.num_output_ids < features.num_outputs:
        return False
    return True


def is_coinjoin_wasabi_1_0(features: TransactionFeatures, epsilon: int = 1000000,
                           max_inputs_per_participants: int = 7, 
                           min_num_participants: int = 5):
    
    anonymity_set = features.occ_most_represented_output_value
    if anonymity_set is None:
        return False
    if anonymity_set < min_num_participants:
        return False
    if anonymity_set > features.num_input_ids:
        return False
    if features.num_inputs > max_inputs_per_participants * anonymity_set:
        return False
    
    if anonymity_set < (features.num_outputs - 1) / 2:
        return False
    
    if features.num_output_ids < features.num_outputs:
        return False
    
    possible_denominations = []
    for value, count in zip(features.output_values, features.occ_output_values):
        if count == anonymity_set:
            possible_denominations.append(value)
    possible_denominations = sorted(possible_denominations, key=lambda x: abs(x - 10000000))
    denomination = possible_denominations[0]
    if (denomination < 10000000 - epsilon) or (denomination > 10000000 + epsilon):
        return False
    
    return True
    
    
def is_coinjoin_wasabi_1_1(features: TransactionFeatures, epsilon: int = 1000000,
                           max_inputs_per_participants: int = 7, 
                           min_num_participants: int = 5,
                           max_mixing_level: int = 12):

    anonymity_set = features.occ_most_represented_output_value
    if anonymity_set is None:
        return False
    if anonymity_set < min_num_participants:
        return False
    if anonymity_set > features.num_input_ids:
        return False
    if features.num_inputs > max_inputs_per_participants * anonymity_set:
        return False

    if features.num_output_ids < features.num_outputs:
        return False
    
    possible_denominations = []
    for value, count in zip(features.output_values, features.occ_output_values):
        if count == anonymity_set:
            possible_denominations.append((value, count))
    possible_denominations = sorted(possible_denominations, key=lambda x: abs(x[0] - 10000000))
    denomination = possible_denominations[0][0]

    if (denomination < 10000000 - epsilon) or (denomination > 10000000 + epsilon):
        return False

    num_post_mix_outputs = possible_denominations[0][1]
    for level in range(1, max_mixing_level):
        alpha_i = 2 ** level * (10000000 - epsilon)
        beta_i = 2 ** level * (10000000 + epsilon)
        for value, count in zip(features.output_values, features.occ_output_values):
            if alpha_i <= value <= beta_i:
                num_post_mix_outputs += count
                
    if num_post_mix_outputs < features.num_outputs - anonymity_set - 1:
        return False
    
    return True
        
    
def is_coinjoin_wasabi_2(features: TransactionFeatures, max_inputs_per_participants: int = 10,
                         min_num_inputs: int = 50, min_input_value: int = 5000):
    
    allowed_denominations = [5000, 6561, 8192, 10000, 13122, 16384, 19683, 20000, 32768, 39366, 50000, 59049, 65536,
                             100000, 118098, 131072, 177147, 200000, 262144, 354294, 500000, 524288, 531441, 1000000,
                             1048576, 1062882, 1594323, 2000000, 2097152, 3188646, 4194304, 4782969, 5000000, 8388608,
                             9565938, 10000000, 14348907, 16777216, 20000000, 28697814, 33554432, 43046721, 50000000,
                             67108864, 86093442, 100000000, 129140163, 134217728, 200000000, 258280326, 268435456,
                             387420489, 500000000, 536870912, 774840978, 1000000000, 1073741824, 1162261467, 2000000000,
                             2147483648, 2324522934, 3486784401, 4294967296, 5000000000, 6973568802, 8589934592,
                             10000000000, 10460353203, 17179869184, 20000000000, 20920706406, 31381059609, 34359738368,
                             50000000000, 62762119218, 68719476736, 94143178827, 100000000000, 137438953472]
    
    if features.num_inputs < min_num_inputs:
        return False
    if len(list(features.input_values)) == 0:
        return False
    if np.min(list(features.input_values)) < min_input_value:
        return False
    if features.num_output_ids < features.num_outputs:
        return False
    
    num_post_mix_outputs = 0
    for value, count in zip(features.output_values, features.occ_output_values):
        if value in allowed_denominations:
            num_post_mix_outputs += count
            
    if num_post_mix_outputs < (features.num_outputs - 1) / 2:
        return False
    
    if num_post_mix_outputs < features.num_inputs / max_inputs_per_participants:
        return False
    
    return True


def is_tx0_samourai(features: TransactionFeatures, epsilon_min: int = 1, epsilon_max: int = 11000,
                    max_num_premix: int = 75, tol_fee_min: float = 0.5, tol_fee_max: float = 3.):
    
    allowed_denominations = [100000, 1000000, 5000000, 50000000]
    pools = {100000: 5000, 1000000: 50000, 5000000: 175000, 50000000: 1750000}
    
    if features.is_position_zero_in_output:
        return False

    pre_mix_denomination = None
    pre_mix_count = None
    post_mix_denomination = None
    
    for value, count in zip(features.output_values, features.occ_output_values):
        for denomination in allowed_denominations:
            if (denomination + epsilon_min) <= value <= (denomination + epsilon_max):
                if pre_mix_denomination is None:
                    pre_mix_denomination, pre_mix_count, post_mix_denomination = value, count, denomination
                elif count > pre_mix_count:
                    pre_mix_denomination, pre_mix_count, post_mix_denomination = value, count, denomination
                elif (count == pre_mix_count) and (value > pre_mix_denomination):
                    pre_mix_denomination, pre_mix_count, post_mix_denomination = value, count, denomination
                else:
                    pass
                
    if pre_mix_denomination is None:
        return False
                
    if pre_mix_count < features.num_outputs - 2:
        return False 
    
    if pre_mix_count > max_num_premix:
        return False
    
    coordinator_fee = pools[post_mix_denomination]
    fee_found = False
    for value in features.output_values:
        if tol_fee_min * coordinator_fee <= value <= tol_fee_max * coordinator_fee:
            fee_found = True
            break 
    
    if not fee_found:
        return False
    
    return True


def is_coinjoin_samourai(features: TransactionFeatures, epsilon_max: int = 20000):
    allowed_denominations = [100000, 1000000, 5000000, 50000000]
    if features.num_inputs != 5:
        return False
    if features.num_input_ids != 5:
        return False
    if features.num_outputs != 5:
        return False
    if features.num_output_ids != 5:
        return False
    if len(features.output_values) > 1:
        return False
    post_mix_denomination = features.output_values[0]
    if post_mix_denomination not in allowed_denominations:
        return False
    found_post_mix = False
    found_pre_mix = False
    for value in features.input_values:
        if (value < post_mix_denomination) or (value > post_mix_denomination + epsilon_max):
            return False 
        if value == post_mix_denomination:
            found_post_mix = True
        else:
            found_pre_mix = True
    if not found_post_mix or not found_pre_mix:
        return False
    return True
    