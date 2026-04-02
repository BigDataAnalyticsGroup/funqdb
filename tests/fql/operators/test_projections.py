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
from fql.operators.projections import project
from tests.lib import _create_testdata


def test_project():
    db: DBF = _create_testdata(frozen=False)
    customers: RF = db.customers

    customers_projected: RF = project(customers, "name").result

    # lens should match, in contrast to relational algebra, duplicate elimination and hence a smaller len of the output
    # cannot happen
    assert len(customers_projected) == len(customers)
    assert type(customers_projected) == RF

    # output must be a copy of the input, not the same object:
    assert customers._uuid != customers_projected._uuid

    # check for different uuids of tuples:
    for i in range(1, len(customers) + 1):
        assert customers[i]._uuid != customers_projected[i]._uuid

    assert "company" in customers[1]
    assert "company" not in customers_projected[1]
