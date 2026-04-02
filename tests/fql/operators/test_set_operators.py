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
from fdm.attribute_functions import DBF, RF
from fql.operators.set_operations import (
    union,
    intersect,
    minus,
    V,
    Ʌ,
    difference,
    cogroup,
)
from tests.lib import _create_testdata


def test_union():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can union the two RFs without any issues, even though
    # they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = union(input_dbf, output_factory=lambda _: RF(), warn_about_duplicate_keys=False).result
        else:
            result = V(input_dbf, output_factory=lambda _: RF(), warn_about_duplicate_keys=False).result

        assert set(result.keys()) == users_keys.union(customers_keys)
        assert len(result) == 5


def test_intersect():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = intersect(input_dbf, output_factory=lambda _: RF()).result
        else:
            result = Ʌ(input_dbf, output_factory=lambda _: RF()).result

        assert set(result.keys()) == users_keys.intersection(customers_keys)
        assert len(result) == 3


def test_minus():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = minus(input_dbf, output_factory=lambda _: RF()).result
        else:
            result = difference(input_dbf, output_factory=lambda _: RF()).result

        assert set(result.keys()) == users_keys.difference(customers_keys)
        assert len(result) == 0

    # delete a key from customers and check that it is now in the result of the minus operator:
    customers.unfreeze()
    del customers[3]
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:
    input_dbf2 = DBF({"users": users, "customers": customers})
    result: RF = minus(input_dbf2, output_factory=lambda _: RF()).result
    assert set(result.keys()) == users_keys.difference(customers_keys)
    assert len(result) == 1


def test_cogroup():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers

    input_dbf = DBF({"users": users, "customers": customers})

    result: RF = cogroup(
        input_dbf,
        output_factory=lambda _: DBF(),  # one factory for the output of the cogroup operator: DBF mapping from keys to nested AFs
        output_factory_nested=lambda _: RF(),  # one factory for the nested AFs in the output: RFs mapping from the input AF's uuid to the input AF's value for that key
    ).result

    assert len(result) == 5
    assert type(result) is DBF
    assert set(result.keys()) == {1, 2, 3, 4, 5}
    assert len(result[1]) == 2
    assert len(result[2]) == 2
    assert len(result[3]) == 2
    assert len(result[4]) == 1
    assert len(result[5]) == 1
