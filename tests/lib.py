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
from fql.operators.filters import fil


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
        frozen=frozen,
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
        frozen=frozen,
        observe_items=observe_items,
    )
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


def _users_customers_DBF(frozen: bool = True) -> DBF:
    return fil(lambda i: i.key in ["users", "customers"], lambda _: DBF())(
        _create_testdata(frozen=frozen)
    )


def _subset_highly_filtered_DBF(frozen: bool = True) -> DBF:
    db: DBF = _create_testdata(frozen=False)
    departments: RF = db.departments
    users: RF = db.users
    # TODO: the syntax is too complicated, need sargable filters that can be easily applied to any attribute function
    return DBF(
        data={
            "departments": departments(name="Dev"),
            # ),
            #            "departments": fil(lambda i: i.value.name == "Dev", lambda _: RF())(
            #                departments
            #            ),
            "users": fil(lambda i: i.value.name == "Horst", lambda _: RF())(users),
        }
    )


def _subset_DBF(whitelist: set[str], frozen: bool = True, observe_items=False) -> DBF:
    return fil(lambda i: i.key in whitelist, lambda _: DBF())(
        _create_testdata(frozen=frozen, observe_items=observe_items)
    )
