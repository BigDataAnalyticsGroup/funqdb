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


from fdm.attribute_functions import TF, RF, DBF
from fdm.schema import Schema
from fql.operators.filters import filter_keys
from faker import Faker


def _create_testdata(
    frozen: bool = False, observe_items: bool = False, add_schemas=False
) -> DBF:
    """Creates test data for unit tests.
    @param frozen: Whether the created data structures should be frozen (read-only).
    @param observe_items: Whether the created data structures should observe item changes.
    @return: A database function (DBF) containing departments and users relations.
    """

    # departments tuples and relation:
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev", "budget": "11M"}, frozen),
            "d2": TF({"name": "Consulting", "budget": "22M"}, frozen),
        },
        lineage=["RF(departments)"],
        frozen=False,
        observe_items=observe_items,
    )
    if add_schemas:
        departments.add_values_constraint(Schema({"name": str, "budget": int}))

    # users tuples and relation:
    users: RF = RF(
        {
            1: TF({"name": "Horst", "yob": 1972, "department": departments.d1}, frozen),
            2: TF({"name": "Tom", "yob": 1983, "department": departments.d1}, frozen),
            3: TF({"name": "John", "yob": 2002, "department": departments.d2}, frozen),
        },
        lineage=["RF(users)"],
        frozen=False,
        observe_items=observe_items,
    ).references("department", departments)
    if frozen:
        users.freeze()
        departments.freeze()

    if add_schemas:
        users.add_values_constraint(Schema({"name": str, "yob": int, "department": TF}))

    # customers tuples and relation:
    customers: RF = RF(
        {
            1: TF({"name": "Tom", "company": "sample company"}, frozen),
            2: TF({"name": "Tom", "company": "example inc"}, frozen),
            3: TF({"name": "John", "company": "whatever gmbh"}, frozen),
            4: TF({"name": "Peter", "company": "Peter, Paul, and Mary"}, frozen),
            5: TF({"name": "Frank", "company": "Masterhorst"}, frozen),
        },
        lineage=["RF(customers)"],
        frozen=frozen,
        observe_items=observe_items,
    )
    if add_schemas:
        customers.add_values_constraint(Schema({"name": str, "company": str}))

    # database of relations:
    db: DBF = DBF(
        {
            "departments": departments,
            "users": users,
            "customers": customers,
        },
        lineage=["DBF()"],
        frozen=frozen,
        observe_items=observe_items,
    )

    return db


def _create_test_data_scalable(
    frozen: bool = False,
    observe_items: bool = False,
    add_schemas=False,
    num_departments: int = 100,
    num_users: int = 1000,
):
    faker: Faker = Faker()

    # departments tuples and relation:
    departments: RF = RF(
        {
            "d"
            + str(i): TF(
                {
                    "name": faker.company(),
                    "budget": str(faker.pyint(10, 30)) + "M",
                },
                frozen,
            )
            for i in range(1, num_departments + 1)
        },
        lineage=["RF(departments)"],
        frozen=False,
        observe_items=observe_items,
    )
    if add_schemas:
        departments.add_values_constraint(Schema({"name": str, "budget": int}))

    # users tuples and relation:
    users: RF = RF(
        {
            i: TF(
                {
                    "name": faker.first_name(),
                    "yob": faker.year(),
                    "department": departments.random_item().value,
                },
                frozen,
            )
            for i in range(1, num_users + 1)
        },
        lineage=["RF(users)"],
        frozen=False,
        observe_items=observe_items,
    ).references("department", departments)

    if frozen:
        users.freeze()
        departments.freeze()

    if add_schemas:
        users.add_values_constraint(Schema({"name": str, "yob": int, "department": TF}))

    # database of relations:
    db: DBF = DBF(
        {
            "departments": departments,
            "users": users,
        },
        lineage=["DBF()"],
        frozen=frozen,
        observe_items=observe_items,
    )

    return db


def _users_customers_DBF(frozen: bool = True) -> DBF:
    return filter_keys(lambda key: key in ["users", "customers"], lambda _: DBF())(
        _create_testdata(frozen=frozen)
    )


def _subset_highly_filtered_DBF(frozen: bool = True) -> DBF:
    db: DBF = _create_testdata(frozen=False)
    departments: RF = db.departments
    users: RF = db.users
    # oopsie, my database looks like a query graph! (that is how it should be)
    # and oopsie: we do not have to repeat the foreign value constraints in such query! (which is how it should be)
    # could be wrapped here with a subdb-operator to reduce DBFs to matching TFs
    return DBF(
        {
            "departments": departments.𝛔(name="Dev"),
            "users": users.𝛔(name="Horst"),
        }
    )


def _subset_DBF(whitelist: set[str], frozen: bool = True, observe_items=False) -> DBF:
    return filter_keys(lambda key: key in whitelist, lambda _: DBF())(
        _create_testdata(frozen=frozen, observe_items=observe_items)
    )
