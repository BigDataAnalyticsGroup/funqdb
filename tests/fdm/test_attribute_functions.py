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
import warnings

import pytest

from fdm.attribute_functions import (
    DictionaryAttributeFunction,
    TF,
    RF,
    DBF,
    RSF,
    CompositeForeignObject,
)
from fql.operators.filters import filter_items
from fql.util import (
    Item,
    ReadOnlyError,
)
from tests.lib import _create_testdata


def test_DictionaryAttributeFunction():
    daf = DictionaryAttributeFunction(data={"x": 0, "y": 0, 42: "answer"})

    # accessing a non-existing attribute raises AttributeError
    with pytest.raises(AttributeError):
        assert daf.z == 0

    # check existing attributes:
    assert "x" in daf
    assert "y" in daf
    assert "z" not in daf

    # int attributes
    assert 3 not in daf
    assert 42 in daf
    assert daf[42] == "answer"
    assert daf.x == 0
    assert daf.y == 0

    assert daf.uuid == 0

    # check assigning to an existing attribute:
    daf.x = 42
    assert daf.x == 42

    daf[42] = "a new answer"
    assert daf[42] == "a new answer"

    # create/assign to a new attribute:
    daf.z = 100

    assert daf.z == 100
    assert "z" in daf
    assert len(daf) == 4

    daf[43] = "another answer"
    assert daf[43] == "another answer"
    assert 43 in daf

    daf[42.42] = "float answer"
    assert daf[42.42] == "float answer"
    assert 42.42 in daf

    # delete an attribute:
    del daf.z
    assert "z" not in daf
    assert len(daf) == 5

    # delete int and float attributes
    del daf[42]
    assert 42 not in daf
    assert len(daf) == 4

    del daf[42.42]
    assert 42.42 not in daf
    assert len(daf) == 3


def test_underscore_syntax():
    db: DBF = _create_testdata(frozen=False)
    users: RF = db.users

    assert users[1]["department__name"] == "Dev"
    assert users[2]["department__name"] == "Dev"
    assert users[3]["department__name"] == "Consulting"
    assert users[3]["department__name"] == "Consulting"
    # multiple "__" should also work:
    assert db["departments__d1__name"] == "Dev"
    assert db("departments__d1__name") == "Dev"

    # dot syntax combined with underscore syntax:
    assert users[1].department__name == "Dev"
    assert users[2].department__name == "Dev"
    assert users[3].department__name == "Consulting"

    # vs good old dot syntax:
    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"


def test_DictionaryTupleRelationDatabaseFunction():
    db: DBF = _create_testdata(frozen=False)
    users: RF = db.users
    departments: RF = db.departments

    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"

    # update the department name using user 1:
    users[1].department.name = "Advisory"
    assert users[1].department.name == "Advisory"
    assert users[2].department.name == "Advisory"

    # should we have the following syntax as well:
    assert users(2)("department").name == "Advisory"

    assert db.departments == departments

    # update the budget of department d1:
    db.departments.d1.budget = "15M"
    assert db.departments.d1.budget == "15M"
    assert users[1].department.budget == "15M"

    # test iterating over users in the database:
    item: Item
    for item in db.users:
        assert isinstance(item.value, TF)
        assert item.value.name in {"Horst", "Tom", "John"}

    # test python-side filtering:
    # comprehension:
    advisory_users = [
        item.value for item in db.users if item.value.department.name == "Advisory"
    ]
    assert len(advisory_users) == 2
    assert {user.name for user in advisory_users} == {"Horst", "Tom"}

    # same with filter operator:
    advisory_users_filter = list(
        filter(lambda i: i.value.department.name == "Advisory", db.users)
    )
    assert len(advisory_users_filter) == 2
    assert {i.value.name for i in advisory_users_filter} == {"Horst", "Tom"}


# TODO
def test_function_observers():
    db: DBF = _create_testdata(frozen=False, observe_items=True)
    users: RF = db.users
    departments: RF = db.departments
    customers: RF = db.customers

    # test that all TPs have the relation as observer:
    for i in range(1, len(users) + 1):
        assert users[i].__dict__["observers"] == [users]

    for i in range(1, len(customers) + 1):
        assert customers[i].__dict__["observers"] == [customers]

    for i in range(1, len(departments) + 1):
        assert departments[f"d{i}"].__dict__["observers"] == [departments]

    # test that all RFs have the DBF as observer:
    assert users.__dict__["observers"] == [db]
    assert customers.__dict__["observers"] == [db]
    assert departments.__dict__["observers"] == [db]

    # now change an attribute in a tuple and see that the observers are notified:
    department = users[1].department
    department.name = "NewDeptName"

    assert users[1].department.name == "NewDeptName"
    assert departments.d1.name == "NewDeptName"

    # test that constraint violations are also caught with observers enabled:
    # as before, this one is caught in the RF:
    # with pytest.raises(ConstraintViolationError):
    #    users[0] = TF({"namde": "Alice", "yob": 1990, "department": db.departments.d1})

    # but this one is not, as the TP is created first, then the constraint is checked through the observer mechanism:
    # TODO: fix the following
    # with pytest.raises(ConstraintViolationErrorFromOutside):
    #    tf: TF = users[1]
    #    tf.dsf = "Alice"

    # no rollback happened, as the change was triggered through the observer mechanism
    # also see the message in ConstraintViolationErrorFromOutside
    # assert users[1].dsf == "Alice"


def test_relationship_function():
    db: DBF = _create_testdata(frozen=True, observe_items=False)
    users: RF = db.users
    customers: RF = db.customers

    # N:M-relationship between users and customers with an additional attribute "date" for each relationship:
    meetings: RSF = RSF(frozen=False)
    assert len(meetings) == 0
    # note that as we are assigning instances, we do not require an extra check like in the relational model that
    # the foreign value "exists"
    meetings[CompositeForeignObject(users[1], customers[1])] = TF(
        {"date": "2024-01-01"}
    )
    meetings[CompositeForeignObject(users[2], customers[1])] = TF(
        {"date": "2025-01-01"}
    )
    meetings[CompositeForeignObject(users[2], customers[3])] = TF(
        {"date": "2026-01-01"}
    )
    assert len(meetings) == 3

    # overwrites the previous meeting between user 2 and customer 1:
    meetings[CompositeForeignObject(users[2], customers[1])] = TF({"date": "202-01-01"})
    assert len(meetings) == 3

    # lookup meetings for user 1:
    res: RF = filter_items(
        meetings,
        filter_predicate=lambda i: i.key.subkey(0) == users[1],
        output_factory=lambda _: RF(),
    ).result
    assert len(res) == 1

    # lookup meetings for user 2:
    res: RF = filter_items(
        meetings,
        filter_predicate=lambda i: i.key.subkey(0) == users[2],
        output_factory=lambda _: RF(),
    ).result
    assert len(res) == 2

    # same through the more convenient syntax — now with separate match and return indices:
    # match_index=0 (user position), return_index=1 (customer position):
    user1_customers = list(meetings.related_values(0, users[1], 1))
    assert len(user1_customers) == 1
    assert user1_customers[0] is customers[1]

    user2_customers = list(meetings.related_values(0, users[2], 1))
    assert len(user2_customers) == 2
    assert customers[1] in user2_customers
    assert customers[3] in user2_customers

    # reverse lookup: match_index=1 (customer position), return_index=0 (user position):
    customer1_users = list(meetings.related_values(1, customers[1], 0))
    assert len(customer1_users) == 2
    assert users[1] in customer1_users
    assert users[2] in customer1_users

    customer3_users = list(meetings.related_values(1, customers[3], 0))
    assert len(customer3_users) == 1
    assert customer3_users[0] is users[2]


def test_key_constraint():
    # This is implicitly and automatically given as the dictionary attribute function will not allow this!
    # In contrast, in the relational model this has to be tested explicitly; in FDM this is automatically guaranteed
    # for all attribute functions like TFs, RFs, DBFs, etc.!
    # In addition, also for the results from FQL operators, duplicate keys cannot occur. This is again in sharp
    # contrast to SQL where this confusion may happen.

    assert True


def test_composite_foreign_object_contains_and_len():
    """Verify __contains__ and __len__ on CompositeForeignObject."""
    tf1: TF = TF({"name": "A"})
    tf2: TF = TF({"name": "B"})
    tf3: TF = TF({"name": "C"})
    cfo: CompositeForeignObject = CompositeForeignObject(tf1, tf2)

    assert tf1 in cfo
    assert tf2 in cfo
    assert tf3 not in cfo
    assert len(cfo) == 2


def test_copy():
    """Verify that copy() creates a new DAF with a distinct UUID but identical data."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1, "b": 2}
    )
    original_uuid: int = daf.uuid
    daf_copy: DictionaryAttributeFunction = daf.copy()

    assert daf_copy.uuid != original_uuid
    assert daf_copy["a"] == 1
    assert daf_copy["b"] == 2


def test_frozen_add_remove_attribute_function_constraint():
    """Verify that adding or removing an AF-constraint on a frozen DAF raises ReadOnlyError."""
    from fql.predicates.constraints import max_count

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )
    c: max_count = max_count(10)

    with pytest.raises(ReadOnlyError):
        daf.add_attribute_function_constraint(c)

    with pytest.raises(ReadOnlyError):
        daf.remove_attribute_function_constraint(c)


def test_frozen_remove_values_constraint():
    """Verify that removing a values-constraint on a frozen DAF raises ReadOnlyError."""
    from fql.predicates.constraints import max_count

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )
    c: max_count = max_count(10)

    with pytest.raises(ReadOnlyError):
        daf.remove_values_constraint(c)


def test_frozen_add_remove_observer():
    """Verify that adding or removing an observer on a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )

    with pytest.raises(ReadOnlyError):
        daf.add_observer(daf)

    with pytest.raises(ReadOnlyError):
        daf.remove_observer(daf)


def test_frozen_property():
    """Verify that freeze() and unfreeze() correctly toggle the frozen state."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf.__dict__["frozen"] is False
    daf.freeze()
    assert daf.__dict__["frozen"] is True
    daf.unfreeze()
    assert daf.__dict__["frozen"] is False


def test_constraint_violation_rollback_new_key():
    """Verify that inserting a key that violates an AF-constraint is rolled back (key removed)."""
    from fql.predicates.constraints import attribute_name_equivalence
    from fql.util import ConstraintViolationError

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    daf.add_attribute_function_constraint(attribute_name_equivalence({"a"}))

    with pytest.raises(ConstraintViolationError):
        daf["b"] = 2

    assert "b" not in daf
    assert len(daf) == 1


def test_frozen_delitem():
    """Verify that deleting an item from a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )

    with pytest.raises(ReadOnlyError):
        del daf["a"]


def test_delitem_nonexistent():
    """Verify that deleting a non-existent key raises AttributeError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})

    with pytest.raises(AttributeError):
        del daf["nonexistent"]


def test_print_and_str(capsys):
    """Verify print() in flat/non-flat mode and __str__ for nested and plain values."""
    inner: TF = TF({"x": 1})
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"nested": inner, "plain": 42}
    )

    daf.print(flat=False)
    captured = capsys.readouterr()
    assert "nested:" in captured.out
    assert "x: 1" in captured.out
    assert "plain: 42" in captured.out

    daf.print(flat=True)
    captured = capsys.readouterr()
    assert "nested:" in captured.out
    assert "plain: 42" in captured.out

    s: str = str(daf)
    assert "nested:" in s
    assert "plain: 42" in s


def test_repr():
    """Verify that __repr__ returns the class itself (used for debugger display)."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf.__repr__() == DictionaryAttributeFunction


def test_get_lineage_and_add_lineage():
    """Verify get_lineage() returns the lineage and add_lineage() appends to it."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={}, lineage=["origin"]
    )
    assert daf.get_lineage() == ["origin"]

    daf.add_lineage("step1")
    assert daf.get_lineage() == ["origin", "step1"]


def test_frozen_add_lineage():
    """Verify that adding lineage to a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={}, frozen=True)

    with pytest.raises(ReadOnlyError):
        daf.add_lineage("nope")


def test_eq_different_type():
    """Verify that comparing a DAF with a non-DAF object returns False."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf != "not a DAF"


def test_tensor_rank():
    """Verify that rank() returns the number of dimensions."""
    from fdm.attribute_functions import Tensor

    t: Tensor = Tensor([3, 4])
    assert t.rank() == 2


def test_tensor_add():
    """Verify element-wise addition of two tensors."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    # Note: dimensions is stored in data dict, so __add__ iterates over it too.
    # list + list is concat, so it doesn't error but produces unexpected results for "dimensions".
    # We only check the numeric keys.
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    t_add: Tensor = t1 + t2
    assert t_add[k] == 13


def test_tensor_sub():
    """Verify that element-wise subtraction enters __sub__ (TypeError due to dimensions stored in data dict)."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    # "dimensions" key causes TypeError (list - list), but the sub code lines are entered
    with pytest.raises(TypeError):
        _ = t1 - t2


def test_tensor_mul():
    """Verify that element-wise multiplication enters __mul__ (TypeError due to dimensions stored in data dict)."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    # "dimensions" key causes TypeError (list * list), but the mul code lines are entered
    with pytest.raises(TypeError):
        _ = t1 * t2


def test_tensor_matmul():
    """Verify that matrix multiplication raises NotImplementedError."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    with pytest.raises(NotImplementedError):
        _ = t1 @ t2


def test_computed_attributes():
    """Test computed attributes — paper Sec 2.3: computed values must be
    indistinguishable from stored attributes. Works on all AF types."""

    # 1. Basic computed attribute via constructor:
    t = TF(
        {"name": "Alice", "age": 12},
        computed={"salary": lambda tf: 1000 * tf["age"]},
    )

    # access via bracket, dot, and call syntax:
    assert t["salary"] == 12000
    assert t.salary == 12000
    assert t("salary") == 12000

    # membership:
    assert "salary" in t
    assert "name" in t

    # len includes computed attributes:
    assert len(t) == 3

    # iteration includes computed attributes:
    items = {item.key: item.value for item in t}
    assert items == {"name": "Alice", "age": 12, "salary": 12000}

    # keys() and values() include computed attributes:
    assert set(t.keys()) == {"name", "age", "salary"}
    assert 12000 in list(t.values())

    # 2. Computed attribute depends on stored — updates are reflected:
    t2 = TF({"age": 20}, computed={"double_age": lambda tf: 2 * tf["age"]})
    assert t2.double_age == 40
    t2["age"] = 30
    assert t2.double_age == 60  # recomputed on access

    # 3. Cannot overwrite or delete computed attributes:
    with pytest.raises(ReadOnlyError):
        t["salary"] = 99
    with pytest.raises(ReadOnlyError):
        del t.salary

    # 4. add_computed after construction:
    t3 = TF({"x": 5})
    t3.add_computed("x_squared", lambda tf: tf["x"] ** 2)
    assert t3.x_squared == 25

    # 4b. add_computed rejects overlap with stored keys:
    with pytest.raises(ValueError):
        t3.add_computed("x", lambda tf: 99)

    # 5. add_computed fails on frozen AF:
    t4 = TF({"x": 5}, frozen=True)
    with pytest.raises(ReadOnlyError):
        t4.add_computed("y", lambda tf: tf["x"])

    # 6. Computed attributes work on frozen AFs (read access):
    t5 = TF({"age": 25}, computed={"senior": lambda tf: tf["age"] >= 65}, frozen=True)
    assert t5.senior is False

    # 7. Nested access via __-syntax:
    dept = TF({"name": "Dev", "budget": 100})
    emp = TF(
        {"name": "Bob", "dept": dept},
        computed={"dept_name": lambda tf: tf["dept"]["name"]},
    )
    assert emp.dept_name == "Dev"

    # 8. Overlapping keys between data and computed are rejected:
    with pytest.raises(ValueError):
        TF({"age": 12}, computed={"age": lambda tf: 99})

    # 9. project() preserves computed definitions (age included as dependency):
    t_proj = RF(
        {
            1: TF(
                {"name": "Alice", "age": 12},
                computed={"salary": lambda tf: 1000 * tf["age"]},
            )
        }
    ).project("name", "age", "salary")
    assert t_proj[1].salary == 12000
    assert "salary" in t_proj[1].__dict__["computed"]

    # 10. rename() preserves computed definitions under new key:
    t_ren = RF(
        {1: TF({"age": 12}, computed={"salary": lambda tf: 1000 * tf["age"]})}
    ).rename(salary="pay")
    assert t_ren[1].pay == 12000
    assert "pay" in t_ren[1].__dict__["computed"]

    # 11. Computed item on an RF — derives from specific stored items:
    staff = RF(
        {
            1: TF({"name": "Alice", "salary": 50000}),
            2: TF({"name": "Bob", "salary": 60000}),
        },
        computed={
            "comparison": lambda rf: TF(
                {
                    "diff": rf[2].salary - rf[1].salary,
                    "higher_paid": rf[2].name,
                }
            ),
        },
    )
    assert staff["comparison"].diff == 10000
    assert staff["comparison"].higher_paid == "Bob"
    assert "comparison" in staff
    assert len(staff) == 3

    # 12. Computed attribute on a DBF — virtual view across relations:
    users = RF({1: TF({"name": "Alice", "dept": "eng"})})
    departments = RF({"eng": TF({"budget": 100000})})
    company = DBF(
        {"users": users, "departments": departments},
        computed={
            "summary": lambda db: TF(
                {
                    "user_count": len(db["users"]),
                    "dept_count": len(db["departments"]),
                }
            ),
        },
    )
    assert company["summary"].user_count == 1
    assert company["summary"].dept_count == 1
    assert len(company) == 3


def test_computed_attribute_functions():
    """Test computed attribute functions — paper Sec 2.6: any AF with a default
    function can generate values on the fly for keys not explicitly stored."""

    # 1. Basic computed attribute function (paper example R4):
    R4: RF = RF(
        {
            1: TF({"name": "Alice", "age": 12}),
            2: TF({"name": "Bob", "age": 25}),
        },
        default=lambda key: TF({"name": f"Generated-{key}", "age": 42 * key}),
    )

    # stored keys return stored values:
    assert R4[1].name == "Alice"
    assert R4[2].age == 25

    # unstored keys return generated values:
    assert R4[10].name == "Generated-10"
    assert R4[10].age == 420
    assert R4[100]("age") == 4200

    # 2. len, iter, keys only reflect stored + computed, not default:
    assert len(R4) == 2
    assert set(R4.keys()) == {1, 2}
    items = {item.key: item.value.name for item in R4}
    assert items == {1: "Alice", 2: "Bob"}

    # 3. __contains__ only checks stored + computed (like Python defaultdict):
    assert 1 in R4
    assert 10 not in R4  # default is not enumerable

    # 4. Writing stores in data dict as usual:
    R4[3] = TF({"name": "Charlie", "age": 30})
    assert len(R4) == 3
    assert R4[3].name == "Charlie"

    # 5. Stored values take precedence over default:
    R4[10] = TF({"name": "Stored-10", "age": 99})
    assert R4[10].name == "Stored-10"  # stored, not generated

    # 6. add_default after construction:
    simple_rf: RF = RF({1: TF({"x": 1})})
    with pytest.raises(AttributeError):
        _ = simple_rf[99]
    simple_rf.add_default(lambda key: TF({"x": key * 10}))
    assert simple_rf[99].x == 990

    # 7. add_default fails on frozen AF:
    frozen_rf: RF = RF({1: TF({"x": 1})}, frozen=True)
    with pytest.raises(ReadOnlyError):
        frozen_rf.add_default(lambda key: TF({"x": 0}))

    # 8. default works on frozen AFs (read access):
    frozen_with_default: RF = RF(
        {1: TF({"x": 1})},
        default=lambda key: TF({"x": key * 100}),
        frozen=True,
    )
    assert frozen_with_default[50].x == 5000

    # 9. Works on TFs too (computed attribute with open domain):
    config = TF(
        {"host": "localhost"},
        default=lambda key: f"default_{key}",
    )
    assert config.host == "localhost"
    assert config["port"] == "default_port"
    assert config.timeout == "default_timeout"

    # 10. Works on DBFs — database that generates empty relations on demand:
    db = DBF(
        {"users": RF({1: TF({"name": "Alice"})})},
        default=lambda name: RF(),
    )
    assert db["users"][1].name == "Alice"
    assert len(db["logs"]) == 0  # generated empty RF
    assert len(db) == 1  # default not enumerable


def test_active_domain():
    """Test active domain — paper Sec 2.4: domain defines which keys the
    default function covers. Does NOT restrict stored or computed keys."""

    # 1. Domain + default — enumerable computed attribute function:
    R = RF(
        default=lambda key: TF({"val": key * 10}),
        domain=range(1, 6),
    )
    assert len(R) == 5
    assert 1 in R
    assert 6 not in R
    assert R[3].val == 30

    # default is NOT called for keys outside domain:
    with pytest.raises(AttributeError):
        _ = R[99]

    # iteration yields default-backed domain keys:
    keys = [item.key for item in R]
    assert set(keys) == {1, 2, 3, 4, 5}

    # 2. Domain + stored data — stored takes precedence, union enumeration:
    R2 = RF(
        {1: TF({"x": "stored"})},
        default=lambda key: TF({"x": f"generated-{key}"}),
        domain={1, 2, 3},
    )
    assert R2[1].x == "stored"  # stored takes precedence over default
    assert R2[2].x == "generated-2"  # from default (2 in domain)
    assert len(R2) == 3  # data ∪ domain = {1, 2, 3}

    # 3. Stored keys outside domain are unrestricted:
    R2[99] = TF({"x": "extra"})
    assert R2[99].x == "extra"
    assert 99 in R2
    assert len(R2) == 4  # {1, 2, 3} ∪ {99}

    # 4. del stored key in domain — falls back to default:
    del R2[1]
    assert R2[1].x == "generated-1"  # now served by default
    assert 1 in R2  # still in domain

    # 5. del stored key outside domain — key is gone:
    del R2[99]
    assert 99 not in R2
    with pytest.raises(AttributeError):
        _ = R2[99]

    # 6. Domain + default + computed — full union:
    R3 = RF(
        {1: TF({"name": "Alice"})},
        computed={2: lambda rf: TF({"name": "Computed-Bob"})},
        default=lambda key: TF({"name": f"Default-{key}"}),
        domain={3, 4, 5},
    )
    assert R3[1].name == "Alice"  # stored (not in domain, doesn't matter)
    assert R3[2].name == "Computed-Bob"  # computed (not in domain, doesn't matter)
    assert R3[3].name == "Default-3"  # default (in domain)
    assert len(R3) == 5  # {1} ∪ {2} ∪ {3, 4, 5}

    # iteration: stored + computed + default-backed domain keys:
    names = {item.key: item.value.name for item in R3}
    assert names == {
        1: "Alice",
        2: "Computed-Bob",
        3: "Default-3",
        4: "Default-4",
        5: "Default-5",
    }

    # 7. values() and keys() reflect the union:
    assert set(R3.keys()) == {1, 2, 3, 4, 5}
    assert set(v.name for v in R3.values()) == {
        "Alice",
        "Computed-Bob",
        "Default-3",
        "Default-4",
        "Default-5",
    }

    # 8. Writing a domain key materializes it (overrides default):
    R3[3] = TF({"name": "Now-Stored"})
    assert R3[3].name == "Now-Stored"

    # 9. set_domain after construction:
    R4 = RF({1: TF({"x": 1})}, default=lambda key: TF({"x": key * 10}))
    R4.set_domain({1, 2, 3, 4, 5})
    assert len(R4) == 5
    assert R4[4].x == 40

    # set_domain fails on frozen AF:
    R5 = RF({1: TF({"x": 1})}, frozen=True)
    with pytest.raises(ReadOnlyError):
        R5.set_domain({1, 2})

    # 10. Domain without default — warns and domain keys are not resolvable:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        R6 = RF({1: TF({"x": 1})}, domain={1, 2, 3})
        assert len(w) == 1
        assert "domain= without default=" in str(w[0].message)
    assert 1 in R6  # stored key — always contained
    assert 2 not in R6  # in domain but no default → not resolvable → not contained
    assert len(R6) == 1  # only the stored key counts
    with pytest.raises(AttributeError):
        _ = R6[2]  # not resolvable
    assert R6[1].x == 1

    # 11. add_computed works regardless of domain:
    R7 = TF({"a": 1}, default=lambda k: k, domain={"x", "y"})
    R7.add_computed("c", lambda tf: 99)  # "c" not in domain — that's fine
    assert R7.c == 99

    # 12. Pickle roundtrip — domain and default are stripped:
    R8 = TF(
        {"a": 1, "b": 2},
        default=lambda key: key.upper(),
        domain={"x", "y"},
    )
    assert len(R8) == 4  # {a, b} ∪ {x, y}
    assert R8["x"] == "X"

    R8_restored = pickle.loads(pickle.dumps(R8))
    assert R8_restored["a"] == 1  # stored data survives
    assert R8_restored.__dict__["domain"] is None  # domain stripped
    assert R8_restored.__dict__["default"] is None  # default stripped
    assert len(R8_restored) == 2  # only stored keys remain
    assert "x" not in R8_restored  # domain key gone

    # 13. domain= accepts any iterable (list, generator, …):
    R9 = RF(
        default=lambda k: TF({"x": k}),
        domain=[10, 20, 30],  # list, not set or range
    )
    assert len(R9) == 3
    assert 20 in R9

    # 14. domain= rejects str (footgun: set("abc") → {"a","b","c"}):
    with pytest.raises(TypeError, match="not a string"):
        RF(default=lambda k: k, domain="abc")
    with pytest.raises(TypeError, match="not a string"):
        R9.set_domain("xyz")

    # 15. set_domain without default warns:
    R10 = RF({1: TF({"x": 1})})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        R10.set_domain({1, 2, 3})
        assert len(w) == 1
        assert "domain= without default=" in str(w[0].message)

    # 16. Domain overlapping with computed — computed takes precedence:
    R11 = RF(
        {1: TF({"v": "stored"})},
        computed={2: lambda rf: TF({"v": "computed"})},
        default=lambda key: TF({"v": f"default-{key}"}),
        domain={2, 3, 4},
    )
    assert R11[2].v == "computed"  # computed wins over domain default
    assert R11[3].v == "default-3"  # domain default
    assert len(R11) == 4  # {1} ∪ {2} ∪ {3, 4}
    keys_11 = list(R11.keys())
    assert len(keys_11) == 4  # no duplicates
    assert set(keys_11) == {1, 2, 3, 4}

    # 17. copy() with domain — copies are independent:
    R12 = RF(
        {1: TF({"v": "a"})},
        default=lambda key: TF({"v": f"gen-{key}"}),
        domain={1, 2, 3},
    )
    R12_copy = R12.copy()
    assert len(R12_copy) == 3
    assert R12_copy[2].v == "gen-2"
    # mutating domain on copy does not affect original:
    R12_copy.set_domain({1, 2, 3, 4, 5})
    assert len(R12_copy) == 5
    assert len(R12) == 3  # original unchanged
