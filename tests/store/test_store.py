import pickle
import uuid

from fdm.API import AttributeFunction
from fdm.attribute_functions import TF
from fql.util import Item
from store.store import Store


def test_pickle_Item(tmp_path):

    file_name: str = str(tmp_path / "test_pickle_Item.pkl")

    AttributeFunction.global_uuid = 42

    # scalar value:
    item: Item = Item(1, "Alice")
    with open(file_name, "wb") as f:
        pickle.dump(item, f)

    with open(file_name, "rb") as f:
        item_loaded: Item = pickle.load(f)

    assert item.key == item_loaded.key
    assert item.value == item_loaded.value

    # same with attribute function as a value:
    item: Item = Item(1, TF({"name": "Alice", "yob": 1990}))
    with open(file_name, "wb") as f:
        pickle.dump(item, f)

    with open(file_name, "rb") as f:
        item_loaded: Item = pickle.load(f)

    assert item.key == item_loaded.key
    assert item.value != item_loaded.value
    assert item_loaded.value == item.value.uuid


def test_pickle_TF(tmp_path):
    file_name: str = str(tmp_path / "test_pickle_TF.sqlite")

    AttributeFunction.global_uuid = 4242

    # pickle a TF directly:
    tf: TF = TF({"name": "Alice", "yob": 1990})
    with open(file_name, "wb") as f:
        pickle.dump(tf, f)

    with open(file_name, "rb") as f:
        tf_loaded: TF = pickle.load(f)

    assert tf == tf_loaded
    assert tf.uuid == tf_loaded.uuid

    # observers:
    observer: TF = TF({"name": "big brother", "role": "observer"})
    tf.add_observer(observer)

    # now with observers:
    with open(file_name, "wb") as f:
        pickle.dump(tf, f)

    with open(file_name, "rb") as f:
        tf_loaded: TF = pickle.load(f)

    assert tf == tf_loaded


def test_sqlitedict(tmp_path):
    # see https://github.com/piskvorky/sqlitedict
    from sqlitedict import SqliteDict

    file_name: str = str(tmp_path / "test_sqlitedict.sqlite")

    AttributeFunction.global_uuid = 4242

    # pickle a TF directly:
    inner_tuple: TF = TF({"name": "Alice", "yob": 1990})
    outer_tuple: TF = TF({"name": "Alice", "nested": inner_tuple})

    customers = SqliteDict(file_name, tablename="customers", autocommit=False)
    # another nesting with an outer dictionary, maybe useful for metadata storage later on, MVCC, etc.?
    # if needed, this should be a dedicated type and not a dict
    customers[outer_tuple.uuid] = {
        "name": "first item",
        "tuple": outer_tuple,
        "version": 1.0,
    }
    customers.commit()
    customers.close()

    customers_reread = SqliteDict(file_name, tablename="customers", autocommit=True)
    assert len(customers_reread) == 1
    assert customers_reread[outer_tuple.uuid]["name"] == "first item"
    assert customers_reread[outer_tuple.uuid]["tuple"].uuid == outer_tuple.uuid

    # should return uuid but not the nested tuple itself:
    assert type(customers_reread[outer_tuple.uuid]["tuple"]["nested"]) == int
    assert customers_reread[outer_tuple.uuid]["tuple"]["nested"] == inner_tuple.uuid
    del customers_reread[outer_tuple.uuid]
    customers_reread.commit()
    customers_reread.close()


"""
def test_SQLLite_custom_serializer():
    # TODO: write custom serializer for TF_SQLLite
    # probably not needed if we use pickle as serializer and change get and setstate accordingly
    # https://pypi.org/project/sqlitedict/
    import json

    with SqliteDict("example.sqlite", encode=json.dumps, decode=json.loads) as mydict:
        mydict["key1"] = {"name": "item1", "value": 42}
        assert mydict["key1"]["name"] == "item1"
        assert mydict["key1"]["value"] == 42
        mydict.commit()

    with SqliteDict("example.sqlite", encode=json.dumps, decode=json.loads) as mydict:
        assert mydict["key1"]["name"] == "item1"
        assert mydict["key1"]["value"] == 42              
"""


def test_AttributeFunctionSentinel():

    class AttributeFunctionSentinel:
        """Uses this as entry in dicts to as sentinel for lazy loading from store.
        So when loading an AttributeFunction from the store, we can have
        AttributeFunctionSentinel instances in place of actual AttributeFunction instances.
        This should be checked when accessing values in AttributeFunctions.
        At that point, the actual AttributeFunction can be fetched from the store.
        The AttributeFunctionSentinel instance is then replaced with the actual AttributeFunction instance.

        Note: the "inverse" problem also exists: when evicting an AttributeFunction from memory,
        we need to replace actual AttributeFunction instances with AttributeFunctionSentinel instances.
        This is not implemented yet.

        Maybe some sort of (weak) ref counting is required to know when to evict an AttributeFunction from memory.

        """

        def __init__(self, id: int):
            self.id = id

    afs: AttributeFunctionSentinel = AttributeFunctionSentinel(42)
    # print(v.get_uuid())


def test_store_get_put(tmp_path):

    file_name: str = str(tmp_path / "test_store_get_put.sqlite")
    AttributeFunction.global_uuid = 4242

    store: Store = Store(file_name=file_name)
    inner_tuple: TF = TF({"name": "Alice", "yob": 1990})
    outer_tuple: TF = TF({"name": "Alice", "nested": inner_tuple})
    # add an observer to test that observers are not stored as actual tuples but as UUIDs:
    # and that outer_tuple does not get modified as a side effect of pickling
    observer: TF = TF({"name": "Alice", "nested": inner_tuple})
    outer_tuple.add_observer(observer)
    store.put(outer_tuple)
    store.close()

    store_read: Store = Store(file_name=file_name)

    assert len(store_read) == 1

    outer_tuple_read: AttributeFunction = store_read.get(outer_tuple.uuid)

    assert outer_tuple_read["name"] == "Alice"

    # nested tuple should be stored as uuid, not the actual tuple:
    assert outer_tuple_read.nested == inner_tuple.uuid

    # observer should also be stored as uuid, not the actual tuple:
    assert outer_tuple_read.observers[0] == observer.uuid

    # original outer tuple still points to the actual inner tuple, not the uuid:
    assert type(outer_tuple.nested) == TF

    # original outer tuple still has observer instance, not just uuid:
    assert type(outer_tuple.observers[0]) == TF

    store_read.close()
