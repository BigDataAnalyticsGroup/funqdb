import pytest

from fql.util import Item, ReadOnlyError


def test_Item():
    item1 = Item(key="a", value=1)
    item2 = Item(key="a", value=1)
    item3 = Item(key="b", value=2)

    assert item1 == item2
    assert item1 != item3
    assert hash(item1) == hash(item2)
    assert hash(item1) != hash(item3)

    with pytest.raises(ReadOnlyError):
        # following line is marked as an error in PyCharm: "Cannot assign to field 'value'"
        # nevertheless, it can be executed and raises the expected exception
        item3.value = 3
