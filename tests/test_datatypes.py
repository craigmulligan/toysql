def from_bin(binary: str):
    chunks = split_into_chunks(binary, 8)
    res = ""

    for chunk in chunks:
        first_bit = chunk[1]
        if first_bit == 0:
            break

        res += chunk[1:]

    return int(res, 2)


def split_into_chunks(v: str, chunk_size: int):
    return [v[i : i + chunk_size] for i in range(0, len(v), chunk_size)]


def test_varint_32():
    get_bin = lambda x, n: format(x, "b").zfill(n)

    def to_bin(v: str, size: int):
        last_chunk_index = size / 8 - 1
        res = ""
        chunk_size = 7
        chunks = split_into_chunks(v, chunk_size)

        for i, chunk in enumerate(chunks):
            first_bit = "1"

            if i == last_chunk_index:
                first_bit = "0"

            res += first_bit + chunk

        return res

    def to_bytes(binary: str):
        return int(binary, 2).to_bytes(len(binary) // 8, byteorder="big")

    def from_bytes(h: bytes):
        return format(int.from_bytes(h, byteorder="big"), "b")

    n = 1000
    s = 16

    v = get_bin(n, s - 2)
    x = to_bin(v, s)
    assert v == "00001111101000"
    assert len(x) == len("1000011101101000")
    assert x == "1000011101101000"
    bytearr = to_bytes(x)
    assert bytearr == b"\x87h"
    assert from_bin(from_bytes(bytearr)) == 1000

    ###
