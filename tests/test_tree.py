from toysql.tree import BPlusTree, Node


def test_node(table):
    tree = BPlusTree(table)

    for i in range(0, 50):
        row = (i, f"fred-{i}", f"fred-{i}@flintstone.com")
        tree.insert(i, row)

    # tree.show()

    v = tree.find(30)

    assert v == (30, "fred-30", "fred-30@flintstone.com")

    rows = tree.traverse()
    assert len(rows) == 50
    assert rows[0] == [0, (0, "fred-0", "fred-0@flintstone.com")]


def test_to_from_bytes(table):
    tree = BPlusTree(table)

    for i in range(0, 50):
        row = (i, f"fred-{i}", f"fred-{i}@flintstone.com")
        tree.insert(i, row)

    # leaf_node = tree.root.values[0]
    # v = tree.root.values[0].to_bytes()

    # node = Node(8, table)
    # node.from_bytes(v)
    # assert node.leaf == leaf_node.leaf
    # assert node.keys == leaf_node.keys
    # assert node.values == leaf_node.values

    root_node = tree.root
    v = root_node.to_bytes()

    node = Node(8, table)
    print(node.leaf)
    node = node.from_bytes(v)
    print(node.leaf)

    assert node.leaf == root_node.leaf
    assert node.keys == root_node.keys
    assert node.values == [34] * 8
