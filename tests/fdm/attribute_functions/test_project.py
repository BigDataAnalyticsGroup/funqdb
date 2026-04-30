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


def test_project_clause():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    users_projected: RF = users.project("name", "department")

    assert type(users_projected) == RF
    assert len(users_projected) == 3
    assert users_projected != users  # different instance
    for value in users_projected.values():
        assert "department" in value
        assert "name" in value
        assert "yob" not in value
