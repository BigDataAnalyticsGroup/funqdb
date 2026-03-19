#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import pickle

from fdm.API import AttributeFunction, AttributeFunctionSentinel
from fdm.attribute_functions import TF
from fql.util import Item
from store.store import Store


def test_pickle_Item(tmp_path):

    file_name: str = str(tmp_path / "test_pickle_Item.pkl")

    AttributeFunction.global_uuid = 42

    # scalar value:
    item: Item = Item(1, "Alice")
    with open(file_name, "wb") as f:
        # noinspection PyTypeChecker
        pickle.dump(item, f)

    with open(file_name, "rb") as f:
        item_loaded: Item = pickle.load(f)

    assert item.key == item_loaded.key
    assert item.value == item_loaded.value

    # same with attribute function as a value:
    item: Item = Item(1, TF({"name": "Alice", "yob": 1990}))
    with open(file_name, "wb") as f:
        # noinspection PyTypeChecker
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
        # noinspection PyTypeChecker
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
        # noinspection PyTypeChecker
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
    assert (
        type(customers_reread[outer_tuple.uuid]["tuple"]["nested"])
        == AttributeFunctionSentinel
    )
    assert customers_reread[outer_tuple.uuid]["tuple"]["nested"].id == inner_tuple.uuid
    del customers_reread[outer_tuple.uuid]
    customers_reread.commit()
    customers_reread.close()


def test_store_get_put_no_sentinel_replacement(tmp_path):

    file_name: str = str(tmp_path / "test_store_get_put_no_sentinel_replacement.sqlite")
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

    store_read: Store = Store(file_name=file_name, add_reference_to_store_on_read=False)

    assert len(store_read) == 1

    outer_tuple_read: AttributeFunction = store_read.get(outer_tuple.uuid)

    assert outer_tuple_read["name"] == "Alice"

    # nested tuple should be stored as AttributeFunctionSentinel, not the actual TF:
    assert type(outer_tuple_read.nested) == AttributeFunctionSentinel
    assert outer_tuple_read.nested.id == inner_tuple.uuid

    # observer should also be stored as AttributeFunctionSentinel, not the actual TF:
    assert type(outer_tuple_read.observers[0]) == AttributeFunctionSentinel
    assert outer_tuple_read.observers[0].id == observer.uuid

    # original outer tuple still points to the actual inner tuple, not the uuid:
    assert type(outer_tuple.nested) == TF

    # original outer tuple still has observer instance, not just uuid:
    assert type(outer_tuple.observers[0]) == TF

    store_read.close()


def test_store_get_put_with_sentinel_replacement(tmp_path):

    file_name: str = str(
        tmp_path / "test_store_get_put_with_sentinel_replacement.sqlite"
    )
    AttributeFunction.global_uuid = 4242

    store: Store = Store(file_name=file_name)
    inner_tuple: TF = TF({"name": "Alice", "yob": 1990}, store=store)
    outer_tuple: TF = TF({"name": "Alice", "nested": inner_tuple}, store=store)

    # add an observer to test that observers are not stored as actual tuples but as UUIDs:
    # and that outer_tuple does not get modified as a side effect of pickling
    observer: TF = TF({"name": "Alice", "nested": inner_tuple}, store=store)
    outer_tuple.add_observer(observer)
    store.put(inner_tuple)
    store.put(outer_tuple)
    assert len(store) == 2
    store.close()

    # open store again on the same file:
    store_read: Store = Store(file_name=file_name)

    assert len(store_read) == 2

    outer_tuple_read: AttributeFunction = store_read.get(outer_tuple.uuid)

    assert outer_tuple_read["name"] == "Alice"
    assert outer_tuple_read.store == store_read

    # nested tuple should be stored as TF, not the actual AttributeFunctionSentinel anymore:
    assert type(outer_tuple_read.nested) == TF
    assert outer_tuple_read.nested.uuid == inner_tuple.uuid

    # observer should also be stored as AttributeFunctionSentinel, not the actual TF:
    assert type(outer_tuple_read.observers[0]) == AttributeFunctionSentinel
    assert outer_tuple_read.observers[0].id == observer.uuid

    # original outer tuple still points to the actual inner tuple, not the uuid:
    assert type(outer_tuple.nested) == TF

    # original outer tuple still has observer instance, not just uuid:
    assert type(outer_tuple.observers[0]) == TF

    store_read.close()

def test_store_dependency_notification(tmp_path):
    file_name = str(tmp_path / "test_dependency.sqlite")

    store = Store(file_name=file_name)

    af1 = TF({"value": 1}, store=store)
    af2 = TF({"value": 2}, store=store)

    store.put(af1)
    store.put(af2)

    store.register_dependency(af1.uuid, af2.uuid)

    # verify registry persisted correctly
    registry = store._get_registry()
    assert str(af1.uuid) in registry
    assert af2.uuid in registry[str(af1.uuid)]

    try:
        store.put(af1)
    except TypeError:
        pass

    store.close()

