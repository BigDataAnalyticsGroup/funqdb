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

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.util import ReadOnlyError


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
