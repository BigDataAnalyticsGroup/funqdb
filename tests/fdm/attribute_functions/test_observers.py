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
from tests.lib import _create_testdata


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
