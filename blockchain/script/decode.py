
import os
import yaml
import binascii


path_op_codes = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hex2tokens.yaml")
dict_op_codes = yaml.load(open(path_op_codes, "r"), Loader=yaml.FullLoader)


def parse_script(script: bytes):
    index, tokens = 0, []
    while index < len(script):
        next_hex = int(hex(script[index]), 16)
        if 0x01 <= next_hex <= 0x4b:
            assert len(script) >= index + 1 + next_hex
            data = script[index+1: index+1+next_hex]
            tokens.append(("data", data))
            index += (1 + next_hex)
        elif next_hex == 0x4c:
            assert len(script) > index + 1
            size = int(hex(script[index+1]), 16)
            assert len(script) >= index + 1 + 1 + size
            data = script[index+1+1: index+1+1+size]
            tokens.append(("data", data))
            index += (1 + 1 + size)
        elif next_hex == 0x4d:
            assert len(script) > index + 1 + 2
            size = int(binascii.hexlify(script[index+1: index+1+2][::-1]), 16)
            assert len(script) >= index + 1 + 2 + size
            data = script[index+1+2: index+1+2+size]
            tokens.append(("data", data))
            index += (1 + 2 + size)
        elif next_hex == 0x4e:
            assert len(script) > index + 1 + 4
            size = int(binascii.hexlify(script[index+1: index+1+4][::-1]), 16)
            assert len(script) >= index + 1 + 2 + size
            data = script[index+1+4: index+1+4+size]
            tokens.append(("data", data))
            index += (1 + 4 + size)
        else:
            tokens.append(("op", script[index: index+1]))
            index += 1
    return tokens


def decode_script(script: bytes = None, tokens: list = None, join: bool = False):
    tokens = parse_script(script) if tokens is None else tokens

    def decode_(x: bytes):
        if x[0] == "data":
            try:
                return x[1].hex()
            except:
                return "NON_HEX_DATA"
        else:
            hex_x = int(hex(x[1][0]), 16)
            return dict_op_codes.get(hex_x, f"OP_UNKNOWN_{hex_x}")

    tokens = list(map(decode_, tokens))
    return " ".join(tokens) if join else tokens


def read_variable_length_integer(script, current_position: int = 0):
    int_ = int(script[current_position])
    if int_ < 0xfd:
        new_position = current_position + 1
        return int_, new_position
    elif int_ == 0xfd:
        int_ = int(binascii.hexlify(script[current_position+1:current_position+1+2][::-1]), 16)
        new_position = current_position + 1 + 2
        return int_, new_position
    elif int_ == 0xfe:
        int_ = int(binascii.hexlify(script[current_position+1:current_position+1+4][::-1]), 16)
        new_position = current_position + 1 + 4
        return int_, new_position
    elif int_ == 0xff:
        int_ = int(binascii.hexlify(script[current_position+1:current_position+1+8][::-1]), 16)
        new_position = current_position + 1 + 8
        return int_, new_position
    else:
        raise ValueError


def decode_witness(witness):
    if len(witness) == 0:
        return []
    num_elements, index = read_variable_length_integer(witness, current_position=0)
    stack = []
    for _ in range(num_elements):
        size, index = read_variable_length_integer(witness, current_position=index)
        data = witness[index: index + size]
        stack.append(data)
        index += size
    return stack
