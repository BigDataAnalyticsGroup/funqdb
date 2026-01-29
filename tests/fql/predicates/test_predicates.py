from fql.predicates.constraints import in_subset


def test_predicates():
    i = in_subset({"a", "b", "c"})
    assert i("a") is True
    assert i("d") is False
    assert i("b") is True
    assert i("c") is True
