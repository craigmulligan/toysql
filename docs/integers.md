At the end of this chapter we will have built out a number of integer types which can serialize integers into hexadecimal bytes for on disk storage. SQLite uses a number of different integer types from fixed unsigned integers to unsigned to variable lengths. 

# Fixed width integers

The first types of integer types we are going to implement are fixed width meaning they will always be `n` bits wide/long. For instance a `Int8` will always be 8 bits wide. For instance the integer 5 as an 8 bit integer represented in binary is `TODO` and `\x05` in hexadecimal . 

Let's start with a test to illustrate the functionality we are after:

```python
def test_int8:
  raw_bytes = b"\x05"
  i = 5
  assert raw_bytes == Int8.to_bytes(i)
  assert 5 == Int8.from_bytes(BytesIO(i))
```
