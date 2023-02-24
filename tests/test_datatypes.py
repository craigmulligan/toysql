from toysql.datatypes import Varint32


def test_varint_32():
    get_bin = lambda x, n: format(x, "b").zfill(n)

    def to_bin(v: str, size: int):
        last_chunk_index = size / 8 - 1
        res = ""
        chunk_size = 7
        chunks = [v[i : i + chunk_size] for i in range(0, len(v), chunk_size)]
        print(chunks)

        for i, chunk in enumerate(chunks):
            first_bit = "1"

            if i == last_chunk_index:
                first_bit = "0"

            res += first_bit + chunk

        return res

    def bin_to_hex(binary: str):
        return hex(int(binary, 2))

    n = 1000
    s = 16

    v = get_bin(n, s - 2)
    x = to_bin(v, s)
    assert v == "00001111101000"
    assert len(x) == len("1000011101101000")
    assert x == "1000011101101000"
    assert bin_to_hex(x) == "0x8768"
