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

from fdm.attribute_functions import DBF, RF, TF
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

    assert "name" in customers_projected[1]


def test_project_path():
    """project(users, 'department.name') should traverse the path and yield a TF
    with only the last-segment key 'name' carrying the nested value.
    """
    # Set up test data — user 1 (Horst) is in department d1 ('Dev'), user 3 (John) in d2 ('Consulting')
    db: DBF = _create_testdata(frozen=False)
    # users RF: each TF has 'name', 'yob', and a nested 'department' TF
    users: RF = db.users

    # Project onto the nested path 'department.name'
    users_projected: RF = project(users, "department.name").result

    # Number of entries must be preserved
    assert len(users_projected) == len(users)
    # Result must still be an RF
    assert type(users_projected) == RF

    # Only the last path segment 'name' should appear in each projected TF
    assert "name" in users_projected[1]
    assert "department" not in users_projected[1]
    assert "yob" not in users_projected[1]

    # Values must come from the nested TF, not from the top-level 'name' attribute
    assert users_projected[1].name == "Dev"
    assert users_projected[3].name == "Consulting"


def test_project_multiple_flat_keys():
    """project onto multiple flat keys keeps all named attributes and drops the rest."""
    # Create test data with customers having 'name' and 'company'
    db: DBF = _create_testdata(frozen=False)
    # customers RF has flat attributes 'name' and 'company'
    customers: RF = db.customers

    # Project onto both flat keys simultaneously
    customers_projected: RF = project(customers, "name", "company").result

    # Entry count must be preserved
    assert len(customers_projected) == len(customers)
    # Both projected keys must be present in each result TF
    assert "name" in customers_projected[1]
    assert "company" in customers_projected[1]
    # Values must match the originals
    assert customers_projected[1].name == customers[1].name
    assert customers_projected[1].company == customers[1].company


def test_project_multiple_path_keys():
    """project onto multiple path keys extracts each nested value under its last-segment name."""
    # Create test data — department TF has 'name' and 'budget'
    db: DBF = _create_testdata(frozen=False)
    # users RF: each TF has a nested 'department' TF with 'name' and 'budget'
    users: RF = db.users

    # Project onto two paths that share the same intermediate segment
    users_projected: RF = project(users, "department.name", "department.budget").result

    # Entry count must be preserved
    assert len(users_projected) == len(users)
    # Both last-segment keys must appear in the result TF
    assert "name" in users_projected[1]
    assert "budget" in users_projected[1]
    # Top-level attributes must be absent
    assert "department" not in users_projected[1]
    assert "yob" not in users_projected[1]
    # Values must come from the nested department TF
    assert users_projected[1].name == "Dev"
    assert users_projected[1].budget == "11M"


def test_project_key_conflict_path_wins():
    """When a flat key and a path key resolve to the same last-segment name, the path key wins."""
    # Create test data — user 1 (Horst) has top-level name='Horst', department.name='Dev'
    db: DBF = _create_testdata(frozen=False)
    # users RF: flat 'name' = user name; path 'department.name' = department name
    users: RF = db.users

    # Both 'name' and 'department.name' resolve to key 'name' in the result TF
    users_projected: RF = project(users, "name", "department.name").result

    # The 'name' key must be present
    assert "name" in users_projected[1]
    # Path key is written after flat key, so the department name overwrites the user name
    assert users_projected[1].name == "Dev"


def test_project_nonexistent_flat_key():
    """Projecting onto a key that does not exist silently produces an empty result TF."""
    # Create test data — customers have 'name' and 'company', no 'ghost'
    db: DBF = _create_testdata(frozen=False)
    # customers RF: attributes are 'name' and 'company' only
    customers: RF = db.customers

    # Project onto a key that is not present in any TF
    customers_projected: RF = project(customers, "ghost").result

    # The outer RF must still contain one entry per customer
    assert len(customers_projected) == len(customers)
    # Each result TF must be empty — missing attributes are simply absent in FDM
    assert len(customers_projected[1]) == 0


def test_project_nonexistent_path():
    """Projecting onto a path where any segment is missing silently skips it."""
    # Create test data — department TF has 'name' and 'budget', no 'ghost'
    db: DBF = _create_testdata(frozen=False)
    # users RF: nested 'department' exists, but 'department.ghost' does not
    users: RF = db.users

    # Non-existent final segment: 'department' exists but 'ghost' is not in it
    projected_bad_leaf: RF = project(users, "department.ghost").result
    # Non-existent root segment: 'ghost' is not a key in the user TF at all
    projected_bad_root: RF = project(users, "ghost.name").result

    # Both cases must preserve the outer RF entry count
    assert len(projected_bad_leaf) == len(users)
    assert len(projected_bad_root) == len(users)
    # Both cases must yield empty TFs — missing path segments are silently skipped
    assert len(projected_bad_leaf[1]) == 0
    assert len(projected_bad_root[1]) == 0


def test_project_deep_path():
    """project resolves paths of depth > 2 (a.b.c) by traversing all intermediate segments."""
    # Build three levels of nesting: RF → TF(contact → TF(address → TF(city)))
    inner: TF = TF({"city": "Berlin"})
    # middle TF contains the innermost TF under 'address'
    middle: TF = TF({"address": inner, "zip": "10115"})
    # persons RF maps key 1 to a TF with nested 'contact'
    persons: RF = RF({1: TF({"contact": middle, "name": "Alice"})})

    # Project onto a three-segment path
    persons_projected: RF = project(persons, "contact.address.city").result

    # Entry count must be preserved
    assert len(persons_projected) == len(persons)
    # 'city' (last segment) must appear in the result TF
    assert "city" in persons_projected[1]
    # Intermediate and sibling keys must be absent
    assert "contact" not in persons_projected[1]
    assert "name" not in persons_projected[1]
    # Value must be the deeply nested city string
    assert persons_projected[1].city == "Berlin"


def test_project_computed_with_dependency():
    """Projecting onto a computed key together with its dependency keeps the lambda live."""
    # Build an RF with a TF that has a computed 'salary' depending on stored 'age'
    tf: TF = TF({"name": "Alice", "age": 30})
    # Add a computed attribute that multiplies age by 1000
    tf.add_computed("salary", lambda t: t["age"] * 1000)
    # Wrap in an RF
    rf: RF = RF({1: tf})

    # Project onto 'salary' and its dependency 'age'
    result: RF = project(rf, "salary", "age").result

    # 'salary' must be present as a computed key in the result TF
    assert "salary" in result[1]
    # 'age' must be present as the stored dependency
    assert "age" in result[1]
    # 'name' was not projected, must be absent
    assert "name" not in result[1]
    # The lambda must still evaluate correctly because 'age' is available
    assert result[1].salary == 30000


def test_project_computed_missing_dependency():
    """Projecting onto a computed key without its dependency preserves the lambda
    but raises AttributeError when the computed value is accessed."""
    # Build a TF with computed 'salary' depending on stored 'age'
    tf: TF = TF({"name": "Alice", "age": 30})
    # Computed attribute references 'age', which will not be in the projection
    tf.add_computed("salary", lambda t: t["age"] * 1000)
    # Wrap in an RF
    rf: RF = RF({1: tf})

    # Project onto 'salary' only — 'age' is deliberately excluded
    result: RF = project(rf, "salary").result

    # 'salary' must still be present (the lambda is preserved, not its value)
    assert "salary" in result[1]
    # 'age' must be absent — it was not in the projection
    assert "age" not in result[1]
    # Accessing 'salary' must fail because its dependency 'age' was projected away
    with pytest.raises(AttributeError):
        _ = result[1].salary


def test_project_computed_not_requested():
    """A computed key that is not listed in the projection is absent from the result."""
    # Build a TF with a stored 'name' and a computed 'salary'
    tf: TF = TF({"name": "Alice", "age": 30})
    # Computed attribute that is not included in the projection
    tf.add_computed("salary", lambda t: t["age"] * 1000)
    # Wrap in an RF
    rf: RF = RF({1: tf})

    # Project onto 'name' only — neither 'salary' nor 'age' is requested
    result: RF = project(rf, "name").result

    # 'name' must be present
    assert "name" in result[1]
    # Computed 'salary' must be absent — it was not listed in the projection
    assert "salary" not in result[1]
    # Stored 'age' must also be absent
    assert "age" not in result[1]
