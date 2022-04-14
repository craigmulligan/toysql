from toysql.tree import BPlusTree


def test_node():
    tree = BPlusTree()

    for i in range(0, 50):
        tree.insert(i, f"my-value-{i}")

    # tree.show()
    v = tree.find(30)

    assert v == "my-value-30"

    rows = tree.traverse()
    assert len(rows) == 50
    assert rows[0] == [0, "my-value-0"]


def test_node():
    tree = BPlusTree()

    for i in range(0, 50):
        tree.insert(i, f"my-value-{i}")

    # tree.show()
    v = tree.find(30)

    assert v == "my-value-30"

    rows = tree.traverse()
    assert len(rows) == 50
    assert rows[0] == [0, "my-value-0"]
