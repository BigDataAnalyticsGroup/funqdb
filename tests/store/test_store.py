import pickle
import uuid

import pytest
from sqlitedict import SqliteDict

from fdm.API import AttributeFunction
from fdm.python import TF
from fql.util import Item
from store.store import Store


def test_pickle_Item():

    file_name: str = "item.pkl"
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


def test_pickle_TF():
    file_name: str = "item.pkl"
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

    # TODO: fix tmp file creation and deletion

    # Use tmp_path to create a temporary directory
    temp_dir = tmp_path / "my_temp_dir"
    temp_dir.mkdir()

    # Create a file inside the temporary directory
    temp_file = temp_dir / "keyvaluestore2.sqlite"

    file_name: str = "item.pkl"
    AttributeFunction.global_uuid = 4242

    # pickle a TF directly:
    inner_tuple: TF = TF({"name": "Alice", "yob": 1990})
    outer_tuple: TF = TF({"name": "Alice", "nested": inner_tuple})

    customers = SqliteDict(temp_file.name, tablename="customers", autocommit=False)
    # another nesting, maybe useful for metadata storage later on, MVCC, etc.?
    customers[outer_tuple.uuid] = {
        "name": "first item",
        "tuple": outer_tuple,
        "version": 1.0,
    }
    customers.commit()
    customers.close()

    customers_reread = SqliteDict(
        temp_file.name, tablename="customers", autocommit=True
    )
    assert len(customers_reread) == 1
    assert customers_reread[outer_tuple.uuid]["name"] == "first item"
    assert customers_reread[outer_tuple.uuid]["tuple"] == outer_tuple
    # should return uuid but not the nested tuple itself:
    assert type(customers_reread[outer_tuple.uuid]["tuple"]["nested"]) == int
    assert customers_reread[outer_tuple.uuid]["tuple"]["nested"] == inner_tuple.uuid
    del customers_reread[outer_tuple.uuid]
    customers_reread.commit()
    customers_reread.close()

    temp_dir.rmdir()


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


def test_Value():

    class AttributeFunctionPointer:
        """Uses this as entry in dicts to (automatically) wrap AttributeFunctions?"""

        def __init__(self, af: AttributeFunction = None):
            self.af = af
            self.uuid = uuid.uuid4()

        def get_uuid(self) -> uuid.UUID:
            """Returns the UUID of the AttributeFunctionPointer."""
            return self.uuid

        def get_AF(self) -> AttributeFunction:
            """Returns the actual value, fetching it from the store if necessary."""
            if self.af is None:
                # fetch from store with UUID
                self.af = store.get(self.uuid)
            return self.af

    v = AttributeFunctionPointer(TF({"name": "Alice"}))
    print(v.get_uuid())


def test_store_get_put():

    store: Store = Store()
    tf1 = TF({"name": "Alice", "yob": 1990})
    store.put(tf1)
    uuid: str = str(tf1.uuid)

    store.close()

    # store_read: Store = Store()
    # for key, value in store_read.sqlite_dict.items():
    #    print(key, value)

    # assert len(store_read) == 1
    # tf1_read: AttributeFunction = store_read.get(uuid)

    # assert tf1_read["name"] == "Alice"
    # assert tf1_read["yob"] == 1990

    # store_read.close()
