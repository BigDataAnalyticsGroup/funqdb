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
"""Example funqDB schema for the ``funqdb-viz`` shell script.

This file defines a small ``departments`` / ``users`` / ``projects`` schema
with foreign value references between the relations and a ``Schema``
attached to each. It exposes a single module-level ``db`` variable so it
can be rendered with::

    scripts/funqdb-viz examples/schema_example.py /tmp/schema.html

and then opened in a browser.
"""

from fdm.attribute_functions import DBF, RF, TF
from fdm.schema import Schema

# departments: a small lookup relation.
departments: RF = RF(
    {
        "d1": TF({"name": "Dev", "budget": 11_000_000}),
        "d2": TF({"name": "Consulting", "budget": 22_000_000}),
    }
)
departments.add_values_constraint(Schema({"name": str, "budget": int}))

# users: each user belongs to a department via a foreign value reference.
users: RF = RF(
    {
        1: TF({"name": "Horst", "yob": 1972, "department": departments.d1}),
        2: TF({"name": "Tom", "yob": 1983, "department": departments.d1}),
        3: TF({"name": "John", "yob": 2003, "department": departments.d2}),
    }
).references("department", departments)
users.add_values_constraint(Schema({"name": str, "yob": int, "department": TF}))

# projects: each project is owned by a user and assigned to a department,
# yielding a graph with two outgoing references from one relation.
projects: RF = (
    RF(
        {
            "p1": TF(
                {"title": "Compiler", "owner": users[1], "department": departments.d1}
            ),
            "p2": TF(
                {"title": "Audit", "owner": users[3], "department": departments.d2}
            ),
        }
    )
    .references("owner", users)
    .references("department", departments)
)
projects.add_values_constraint(Schema({"title": str, "owner": TF, "department": TF}))

# The ``funqdb-viz`` script looks for a module-level variable named ``db``.
db: DBF = DBF(
    {
        "departments": departments,
        "users": users,
        "projects": projects,
    }
)
