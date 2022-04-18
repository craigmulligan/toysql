from toysql.tree import BPlusTree, Node


def test_node(table):
    total = 50
    # TODO this fails if re-init the tree eg BPlusTree(table)
    tree = table.tree

    for i in range(0, total):
        row = (i, f"fred-{i}", f"fred-{i}@flintstone.com")
        tree.insert(i, row)

    tree.show()

    search_key = int(total / 2)
    v = tree.find(search_key)

    assert v == (search_key, f"fred-{search_key}", f"fred-{search_key}@flintstone.com")

    rows = tree.traverse()
    assert len(rows) == total
    assert rows[0] == [0, (0, "fred-0", "fred-0@flintstone.com")]
    assert len(table.pager) == 15


def test_to_from_bytes(table):
    tree = table.tree

    for i in range(0, 20):
        row = (i, f"fred-{i}", f"fred-{i}@flintstone.com")
        tree.insert(i, row)

    leaf_node_page_num = tree.root.values[0]
    leaf_node = Node.read(table, leaf_node_page_num)
    v = leaf_node.to_bytes()

    node = Node(table, 0)
    node.from_bytes(v)
    assert node.leaf == leaf_node.leaf
    assert node.keys == leaf_node.keys
    assert node.values == leaf_node.values

    root_node = tree.root
    v = root_node.to_bytes()

    node = Node(table, 0)
    node.from_bytes(v)

    assert node.leaf == root_node.leaf
    assert node.keys == root_node.keys
    assert len(node.values) == len(node.keys)


#    assert table.pager.read(0) == root_node.to_bytes()
