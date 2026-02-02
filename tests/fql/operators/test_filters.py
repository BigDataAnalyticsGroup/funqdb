import tempfile
import uuid

import pytest

from fdm.API import AttributeFunction
from fdm.python import TF, RF, DBF
from fdm.sqlitedict import TF_SQLLite
from fql.operators.APIs import Operator
from fql.operators.filters import filter_items_scan, filter_items
from fql.util import Item
from tests.lib import _create_testdata, _subset_DBF


def test_filter_items():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_reused_and_chained():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(filter_RF(users))  # apply filter instance twice
    filter_RF(filter_RF(users)).explain()

    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_explain():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    print(users.get_lineage())

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    filter_RF2: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "bla",
        output_factory=lambda _: RF(),
    )

    ret1: RF = filter_RF(users, create_lineage=True)
    ret2: RF = filter_RF2(ret1, create_lineage=True)
    print("ret2 lineage:")
    lineage: list[str] = ret2.get_lineage()
    for i, lin in enumerate(lineage, 1):
        print(f"{i}.\t->", lin)


def test_filter_items_complement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate_complement(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
        filter_predicate=filter_predicate_complement,
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 1
    for item in users_filtered:
        assert item.value.department.name == "Consulting"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"John"}


def test_DB_filter_keys():
    # get subdatabase:
    db_filtered: DBF = _subset_DBF({"users", "departments"}, frozen=True)

    assert type(db_filtered) == DBF
    assert len(db_filtered) == 2  # users and departments relations only

    assert type(db_filtered.users) == RF
    assert type(db_filtered.departments) == RF


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
    # tf_attr_func["user1"] = tf1
    # TODO: write custom serializer for TF_SQLLite
    # https://pypi.org/project/sqlitedict/

    # hmm, this does not work like this
    # maybe take control of the pickling process?
    # TODO: how to map the object graph correctly to the underlying key/value store?
    # values that are AttributeFunctions themselves need to be stored separately and linked
    # memory refs must be swizzled correctly
    # this in turn requires AFs to have an identity which can be referenced
    # maybe via UUIDs?
    # https://realpython.com/ref/stdlib/uuid/


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


def test_mangling():
    class A:
        def __init__(self):
            self.__private_attr = 42

        def get_private_attr(self):
            return self.__private_attr

    a = A()
    print(vars(a))
    print(a.__dict__)
