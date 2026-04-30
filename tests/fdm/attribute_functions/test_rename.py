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
from tests.lib import _create_testdata


def test_rename():
    """Verify that rename() renames keys in each value of the AF."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    users_renamed: RF = users.rename(name="first_name", yob="birth_year")

    assert type(users_renamed) == RF
    assert len(users_renamed) == 3
    for value in users_renamed.values():
        assert "first_name" in value
        assert "birth_year" in value
        assert "department" in value
        assert "name" not in value
        assert "yob" not in value

    assert users_renamed[1].first_name == "Horst"
    assert users_renamed[1].birth_year == 1972


def test_rename_alias():
    """Verify that ρ() is an alias for rename()."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    users_renamed: RF = users.ρ(name="first_name")

    assert len(users_renamed) == 3
    for value in users_renamed.values():
        assert "first_name" in value
        assert "name" not in value


def test_rename_no_args_raises():
    """rename() with no arguments raises ValueError."""
    # Create test data
    db: DBF = _create_testdata(frozen=False)
    # Use any RF — the error must fire before any iteration
    users: RF = db.users

    # Calling rename() without mappings must raise ValueError
    with pytest.raises(ValueError):
        users.rename()


def test_rename_non_daf_value_raises():
    """rename() raises TypeError when a value in the AF is not a DictionaryAttributeFunction."""
    # Build an RF whose value is a plain string, not a TF
    scalar_rf: RF = RF({1: "not_a_tf"})

    # rename() must detect the non-DAF value and raise TypeError
    with pytest.raises(TypeError):
        scalar_rf.rename(foo="bar")


def test_rename_produces_copy():
    """rename() returns a new AF; the original is not modified."""
    # Create test data
    db: DBF = _create_testdata(frozen=False)
    # users RF has entries with 'name', 'yob', 'department'
    users: RF = db.users

    # Rename 'name' to 'first_name'
    renamed: RF = users.rename(name="first_name")

    # Result must be a distinct object from the input
    assert renamed._uuid != users._uuid
    # Inner TFs must also be distinct objects
    assert renamed[1]._uuid != users[1]._uuid
    # Original must be unchanged
    assert "name" in users[1]
    assert "first_name" not in users[1]


def test_rename_nonexistent_source_key_ignored():
    """rename() silently ignores a mapping whose source key is not present in the TF."""
    # Create test data — customers have 'name' and 'company', no 'ghost'
    db: DBF = _create_testdata(frozen=False)
    # customers RF: 'name' and 'company' are the only flat attributes
    customers: RF = db.customers

    # Rename a key that does not exist — must not raise, must not affect other keys
    result: RF = customers.rename(ghost="phantom")

    # All original keys must still be present and unchanged
    assert "name" in result[1]
    assert "company" in result[1]
    # The phantom key must not appear
    assert "phantom" not in result[1]
    # Values must be identical to the originals
    assert result[1].name == customers[1].name
    assert result[1].company == customers[1].company


def test_rename_key_collision():
    """When a source key is renamed to a target key that already exists, the last
    write wins. Because rename() iterates data in insertion order, the winner depends
    on which key appears later in the TF. This test uses an inline TF with a fixed
    insertion order so the outcome is deterministic and independent of fixture changes.
    """
    # Build a TF where 'a' is inserted before 'b', then rename 'a' → 'b'.
    # Iteration order: key='a' → renamed['b'] = 1  (written first)
    #                  key='b' → renamed['b'] = 2  (overwrites — 'b' wins)
    rf: RF = RF({1: TF({"a": 1, "b": 2})})

    # Rename 'a' → 'b': collision with the existing 'b' key
    result: RF = rf.rename(a="b")

    # 'a' must be absent — it was renamed away
    assert "a" not in result[1]
    # 'b' holds the original value 2, not the renamed value 1
    assert result[1].b == 2


def test_rename_path_key_raises():
    """rename() raises ValueError when given a dot-separated path key.
    Path-based rename is not supported; the error prevents silent no-ops
    that would otherwise mask caller mistakes.
    """
    # Build an RF whose TFs have a nested 'department' TF with a 'name' attribute
    db: DBF = _create_testdata(frozen=False)
    # users RF: each TF has 'name', 'yob', and a nested 'department' TF
    users: RF = db.users

    # Attempting to rename via a path key must raise ValueError — path-based
    # rename is not supported and silently ignoring it would hide caller mistakes
    with pytest.raises(ValueError):
        users.rename(**{"department.name": "dept_name"})
