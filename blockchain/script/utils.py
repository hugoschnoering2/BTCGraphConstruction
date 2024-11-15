
from blockchain.account import is_pk, is_compressed_pk
from blockchain.script.decode import dict_op_codes


checksig_op = {b"\xac", b"\xad", b"\xae", b"\xaf"}  # operators that check whether the input signatures are valid
hash_op = {b"\xa6", b"\xa7", b"\xa8", b"\xa9", b"\xaa"}
invalid_op = {b"\x50", b"\x89", b"\x8a", b"\xfd", b"\xfe", b"\xff",
              b"\x6a"}  # operators that mark the transaction as invalid if they are executed
conditional_op = {b"\x63", b"\x64"}  # operators at the beginning of a conditional flow
nop_op = {b"\x61", b"\xb0", b"\xb1", b"\xb2", b"\xb3", b"\xb4", b"\xb5", b"\xb6", b"\xb7", b"\xb8",
          b"\xb9"}  # nop operators
num_above_1_op = {b"\x51", b"\x52", b"\x53", b"\x54", b"\x55", b"\x56", b"\x57", b"\x58", b"\x59", b"\x5a", b"\x5b",
                  b"\x5c", b"\x5d", b"\x5e", b"\x5f",
                  b"\x60"}  # operators that put a number above 2 on the top of the stack
all_num_op = num_above_1_op.union({b"\x00", b"\x4f"})  # operators that put a number on the top of the stack
cryptohash_op = {b"\xa6", b"\xa7", b"\xa8", b"\xa9", b"\xaa"}  # operators that compute a hash


def not_protected(tokens: list):
    if len(tokens) == 0:
        return True
    if tokens[-1][0] == "data":
        return True
    if (tokens[-1][0] == "op") and (tokens[-1][0] == b"\x4f"):
        return True
    if (tokens[-1][0] == "op") and (tokens[-1][0] in num_above_1_op):
        return True
    index_check_op = [i for i, token in enumerate(tokens) if (token[0] == "op") and (token[1] in checksig_op)]
    if len(index_check_op) == 0:
        return True
    if len(index_check_op) == 1:
        index = index_check_op[0]
        for ind in range(index):
            if (tokens[ind][0] == "data") and (is_pk(tokens[ind][1]) or is_compressed_pk(tokens[ind][1])):
                return False
            elif (tokens[ind][0] == "op") and (tokens[ind][1] in hash_op):
                return False
        return True
    return False


def not_spendable(tokens: list):
    if (tokens[-1][0] == "op") and (tokens[-1][0] == b"\x00"):
        return True
    tokens_op = set([token[1] for token in tokens if token[0] == "op"])
    if len(tokens_op.intersection(conditional_op)) == 0:
        if len(tokens_op.intersection(invalid_op)) > 0:
            return False
        if b"\x68" not in tokens_op:
            return False
    for token in tokens_op:
        if not(int(hex(token[0]), 16) in dict_op_codes):
            return True
    return False


def compute_equivalent(tokens: list):
    tokens = [token for token in tokens if not (token[0] == "op" and token[1] in nop_op)]
    while True:
        if (tokens[0][0] == "op") and ((tokens[0][1] == b"\x75") or (tokens[0][1] == b"\x6d")):
            tokens = tokens[1:]
            continue
        if (tokens[0][0] == "data") and (tokens[1][0] == "op") and (tokens[1][1] == b"\x6d"):
            tokens = tokens[2:]
            continue
        if ((tokens[0][0] == "op") and (tokens[0][1] in all_num_op) and (tokens[1][0] == "op")
                and (tokens[1][1] == b"\x6d")):
            tokens = tokens[2:]
            continue
        n = len(tokens)
        for i, token in enumerate(tokens):
            if (i > 0) and (token[0] == "op") and (token[1] == b"\x75"):
                if (tokens[i-1][0] == "data") or (tokens[i-1][1] in all_num_op):
                    tokens = tokens[:i-1] + tokens[i+1:]
                    break
            if (i > 1) and (token[0] == "op") and (token[1] == b"\x6d"):
                if (tokens[i-1][0] == "data") or (tokens[i-1][1] in all_num_op):
                    if (tokens[i-2][0] == "data") or (tokens[i-2][1] in all_num_op):
                        tokens = tokens[:i-2] + tokens[i+1:]
                        break
        if len(tokens) < n:
            continue
        break
    return tokens
