import pytest

from lib.core import DictionaryAttributeFunction


def test_DictionaryAttributeFunction():
    daf = DictionaryAttributeFunction()
    assert daf.x == 0
    assert daf.y == 0

    # accessing a non-existing attribute raises AttributeError
    with pytest.raises(AttributeError):
        assert daf.z == 0

    daf.x = 42
    assert daf.x == 42
    assert "x" in daf
    assert "y" in daf
    assert "z" not in daf

    # create a new attribute:
    daf.z = 100
    assert daf.z == 100
    assert "z" in daf
    assert len(daf) == 3

    # delete an attribute:
    del daf.z
    assert len(daf) == 2
