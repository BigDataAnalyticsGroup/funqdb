import pickle
import uuid

import pytest
from sqlitedict import SqliteDict

from fdm.API import AttributeFunction
from fdm.python import TF
from fdm.sqlitedict import TF_SQLLite
from fql.util import Item
from store.store import Store


def test_sqlitedict(tmp_path):
    # see https://github.com/piskvorky/sqlitedict
    from sqlitedict import SqliteDict

    # TODO: fix tmp file creation and deletion

    # Use tmp_path to create a temporary directory
    temp_dir = tmp_path / "my_temp_dir"
    temp_dir.mkdir()

    # Create a file inside the temporary directory
    temp_file = temp_dir / "keyvaluestore.sqlite"

    customers = SqliteDict(temp_file.name, tablename="customers", autocommit=False)
    customers["1"] = {"name": "first item", "bla": 42}
    customers["2"] = {"name": "second item"}
    customers.commit()
    customers["4"] = {"name": "yet another item"}
    customers.close()

    customers = SqliteDict(temp_file.name, tablename="customers", autocommit=True)
    assert "1" in customers
    assert "2" in customers
    print("There are %d items in the database" % len(customers))
    assert len(customers) == 2
    for key in customers:
        print(key, ":", customers[key])
        assert type(customers[key]) == dict
    customers.close()

    users = SqliteDict(temp_file.name, tablename="users", autocommit=True)
    print("There are %d items in the users database" % len(users))
    assert len(users) == 0
    users.close()

    temp_dir.rmdir()


def test_SQLLiteDictAttributeFunction(tmp_path):
    from fdm.sqlitedict import SQLLiteDictAttributeFunction

    attr_func = SQLLiteDictAttributeFunction[str, dict](
        sqlite_file_name="bla.sqlite", frozen=False
    )
    attr_func["key1"] = {"name": "item1"}
    assert attr_func["key1"]["name"] == "item1"

    attr_func2 = SQLLiteDictAttributeFunction[str, dict](
        sqlite_file_name="bla.sqlite", frozen=False
    )
    assert attr_func2["key1"]["name"] == "item1"

    with pytest.raises(AttributeError):
        assert attr_func2.z == 0

    # TODO: more tests


def test_TF_SQLLite():
    from fdm.sqlitedict import SQLLiteDictAttributeFunction

    tf_attr_func = SQLLiteDictAttributeFunction[str, TF_SQLLite](
        sqlite_file_name="tf_sqlite.sqlite", frozen=False, tablename="users"
    )
    tf1 = TF_SQLLite(
        sqlite_file_name="tf_sqlite.sqlite", frozen=False, tablename="tuples"
    )
    tf1["name"] = "Test User"
    tf1["department"] = "Dev"
    # TODO: following does not work
    # tf_attr_func["user1"] = tf1
    # hmm, this does not work like this
    # maybe take control of the pickling process?
    # TODO: how to map the object graph correctly to the underlying key/value store?
    # values that are AttributeFunctions themselves need to be stored separately and linked
    # memory refs must be swizzled correctly
    # this in turn requires AFs to have an identity which can be referenced
    # maybe via UUIDs?
    # https://realpython.com/ref/stdlib/uuid/


"""def test_pickle_Item():
    # scalar value:
    item: Item = Item(1, "Alice")
    with open("item.pkl", "wb") as f:
        pickle.dump(item, f)

    with open("item.pkl", "rb") as f:
        item_loaded: Item = pickle.load(f)

    assert item.key == item_loaded.key
    assert item.value == item_loaded.value

    # AF value
    item: Item = Item(1, TF({"name": "Alice", "yob": 1990}))
    with open("item.pkl", "wb") as f:
        pickle.dump(item, f)

    with open("item.pkl", "rb") as f:
        item_loaded: Item = pickle.load(f)

    assert item.key == item_loaded.key
    assert item.value != item_loaded.value
    assert item_loaded.value == item.value.uuid"""


def test_SQLLite_custom_serializer():
    # TODO: write custom serializer for TF_SQLLite
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

    store_read: Store = Store()
    for key, value in store_read.sqlite_dict.items():
        print(key, value)

    assert len(store_read) == 1
    tf1_read: AttributeFunction = store_read.get(uuid)

    assert tf1_read["name"] == "Alice"
    assert tf1_read["yob"] == 1990

    store_read.close()
