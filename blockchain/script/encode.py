

def unparse_script(tokens: list):
    script = bytearray(b"")
    for token in tokens:

        if token[0] == "data":
            n = len(token[1])
            if n <= 75:
                script += bytearray(n.to_bytes(1, byteorder="little"))
            elif n < 256:
                script += bytearray(b"\x4c")
                script += bytearray(n.to_bytes(1, byteorder="little"))
            elif n < 65536:
                script += bytearray(b"\x4d")
                script += bytearray(n.to_bytes(2, byteorder="little"))
            elif n < 4294967296:
                script += bytearray(b"\x4e")
                script += bytearray(n.to_bytes(4, byteorder="little"))
            else:
                raise ValueError
        script += bytearray(token[1])
    return bytes(script)
