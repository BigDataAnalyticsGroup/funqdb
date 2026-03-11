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

from fdm.attribute_functions import DBF, RF
from tests.lib import _create_test_data_scalable


def test_create_data_scalable():
    """Test creating data with different sizes."""

    for num_departments in [1000]:
        for num_users in [100]:
            db: DBF = _create_test_data_scalable(
                frozen=True,
                num_departments=num_departments,
                num_users=num_users,
            )
            assert type(db) == DBF
            assert len(db) == 2
            assert hasattr(db, "departments")
            assert hasattr(db, "users")
            assert type(db.departments) == RF
            assert type(db.users) == RF
            assert len(db.departments) == num_departments
            assert len(db.users) == num_users
