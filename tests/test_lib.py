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
from tests.lib import _create_test_data_scalable, _create_testdata


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


def test_create_testdata_with_schemas():
    """Verify that _create_testdata works with add_schemas=True, adding Schema constraints."""
    db: DBF = _create_testdata(frozen=False, add_schemas=True)
    assert type(db) == DBF
    assert len(db) == 3
    assert len(db.users) == 3
    assert len(db.departments) == 2
    assert len(db.customers) == 5


def test_create_test_data_scalable_with_schemas():
    """Verify that _create_test_data_scalable works with add_schemas=True."""
    db: DBF = _create_test_data_scalable(
        frozen=False,
        add_schemas=True,
        num_departments=5,
        num_users=10,
    )
    assert type(db) == DBF
    assert len(db.departments) == 5
    assert len(db.users) == 10
