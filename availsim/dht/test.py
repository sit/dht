import dhash
# Do blocks go to the right place?
def test_insert (gdh, blockid, expected):
    gdh.insert_block (4, blockid, 8192)
    assert blockid in gdh.allnodes[expected].blocks

if __name__ == '__main__':
    gdh = dhash.dhash_replica ()
    gdh.add_node (0, 55)
    gdh.add_node (1, 4)
    gdh.add_node (2, 23)
    gdh.add_node (2, 30)
    gdh.add_node (3, 17)
    gdh.add_node (5, 42)
    gdh.add_node (7, 63)
    try:
        gdh.add_node (8, 4)
        assert 0, "Should not allow duplicate insert!"
    except RuntimeError:
        pass

    # Make sure list is sorted
    start = 0
    for n in gdh.nodes:
        assert n.id > start
        start = n.id

    test_insert (gdh, 73, 4)
    test_insert (gdh, 3, 4)
    test_insert (gdh, 4, 4)
    test_insert (gdh, 20, 23)
    test_insert (gdh, 56, 63)

    assert gdh.find_predecessor_index (4) == 6
    assert gdh.find_predecessor_index (66) == 6
    assert gdh.find_predecessor_index (10) == 0
    assert gdh.find_predecessor_index (25) == 2

    assert len(gdh.succ(0, 3)) == 3
    assert len(gdh.succ(63, 3)) == 3
    assert len(gdh.succ(24, 3)) == 3
    assert len(gdh.succ(35, 3)) == 3

    assert gdh.pred (5)[0].id == 4
    assert gdh.pred (31)[0].id == 30
    assert gdh.pred (30)[0].id == 23
    assert gdh.pred (3)[0].id == 63
    assert len(gdh.pred(5, 3)) == 3


