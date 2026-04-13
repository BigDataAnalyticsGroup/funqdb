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

"""POC tests for structured predicates in FQL.

These tests verify that the structured predicate classes (``Eq``, ``Gt``,
``Lt``, ``Gte``, ``Lte``, ``Like``, ``In``, ``And``, ``Or``, ``Not``) are
callable drop-in replacements for lambdas, that they serialize correctly
in the plan IR (as structured dicts, not ``Opaque`` markers), and that they
integrate with ``filter_values`` for end-to-end filtering.

All test methods are AI-generated and marked with
``@pytest.mark.needs_review_new``.
"""

from typing import Any

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.filters import filter_values
from fql.plan import LogicalPlan, Opaque, PlanNode, extract_plan
from fql.plan.ir import PlanChild, _value_from_dict, _value_to_dict
from fql.predicates.predicates import (
    Predicate,
    Ref,
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    In,
    And,
    Or,
    Not,
)
from tests.lib import _create_testdata

# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _make_users_rf() -> tuple[RF, TF, TF, TF, TF, TF]:
    """Build a small RF with three users pointing to two departments.

    Returns (users_rf, dept_dev, dept_hr, user_horst, user_tom, user_john).
    """
    dept_dev: TF = TF({"name": "Dev"}, frozen=True)
    dept_hr: TF = TF({"name": "HR"}, frozen=True)

    user_horst: TF = TF(
        {"name": "Horst", "yob": 1972, "department": dept_dev}, frozen=True
    )
    user_tom: TF = TF({"name": "Tom", "yob": 1983, "department": dept_dev}, frozen=True)
    user_john: TF = TF(
        {"name": "John", "yob": 2003, "department": dept_hr}, frozen=True
    )

    users_rf: RF = RF(
        {1: user_horst, 2: user_tom, 3: user_john},
        frozen=True,
    )
    return users_rf, dept_dev, dept_hr, user_horst, user_tom, user_john


# ---------------------------------------------------------------------------
# 1. Eq — flat attribute
# ---------------------------------------------------------------------------


def test_eq_flat_attribute():
    """Eq('name', 'Horst') must match only the TF whose name is 'Horst'."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Eq = Eq("name", "Horst")

    assert pred(user_horst) is True
    assert pred(user_tom) is False
    assert pred(user_john) is False


# ---------------------------------------------------------------------------
# 2. Eq — nested dot notation
# ---------------------------------------------------------------------------


def test_eq_nested_dot_notation():
    """Eq('department.name', 'Dev') must traverse the RF reference to
    compare the department's name attribute."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Eq = Eq("department.name", "Dev")

    assert pred(user_horst) is True
    assert pred(user_tom) is True
    assert pred(user_john) is False


# ---------------------------------------------------------------------------
# 3. Eq — nested dunder notation
# ---------------------------------------------------------------------------


def test_eq_nested_dunder_notation():
    """Eq('department__name', 'Dev') must produce the same results as
    dot notation — both are normalized to the same attribute path."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Eq = Eq("department__name", "Dev")

    assert pred(user_horst) is True
    assert pred(user_tom) is True
    assert pred(user_john) is False


# ---------------------------------------------------------------------------
# 4. Attribute-to-attribute comparison via Ref
# ---------------------------------------------------------------------------


def test_eq_attribute_to_attribute():
    """Gt('end_year', Ref('start_year')) must compare two attributes on the
    same TF value, returning True only when end_year > start_year."""
    event_ok: TF = TF({"start_year": 2020, "end_year": 2025}, frozen=True)
    event_bad: TF = TF({"start_year": 2025, "end_year": 2020}, frozen=True)
    event_equal: TF = TF({"start_year": 2022, "end_year": 2022}, frozen=True)

    pred: Gt = Gt("end_year", Ref("start_year"))

    assert pred(event_ok) is True
    assert pred(event_bad) is False
    assert pred(event_equal) is False


# ---------------------------------------------------------------------------
# 5. Gt, Lt, Gte, Lte — comparison predicates
# ---------------------------------------------------------------------------


def test_gt_lt_gte_lte():
    """All four comparison predicates must behave correctly on numeric
    attributes, matching the standard Python comparison semantics."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()

    # Gt: yob > 1980
    pred_gt: Gt = Gt("yob", 1980)
    assert pred_gt(user_horst) is False  # 1972
    assert pred_gt(user_tom) is True  # 1983
    assert pred_gt(user_john) is True  # 2003

    # Lt: yob < 1990
    pred_lt: Lt = Lt("yob", 1990)
    assert pred_lt(user_horst) is True  # 1972
    assert pred_lt(user_tom) is True  # 1983
    assert pred_lt(user_john) is False  # 2003

    # Gte: yob >= 1983
    pred_gte: Gte = Gte("yob", 1983)
    assert pred_gte(user_horst) is False  # 1972
    assert pred_gte(user_tom) is True  # 1983
    assert pred_gte(user_john) is True  # 2003

    # Lte: yob <= 1983
    pred_lte: Lte = Lte("yob", 1983)
    assert pred_lte(user_horst) is True  # 1972
    assert pred_lte(user_tom) is True  # 1983
    assert pred_lte(user_john) is False  # 2003


# ---------------------------------------------------------------------------
# 6. Like — prefix matching
# ---------------------------------------------------------------------------


def test_like_prefix():
    """Like('name', 'H%') must match names starting with 'H'."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Like = Like("name", "H%")

    assert pred(user_horst) is True  # "Horst"
    assert pred(user_tom) is False  # "Tom"
    assert pred(user_john) is False  # "John"


# ---------------------------------------------------------------------------
# 7. Like — suffix and contains
# ---------------------------------------------------------------------------


def test_like_suffix_and_contains():
    """Like with suffix ('%ost') and contains ('%or%') patterns must match
    the expected substrings."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()

    # Suffix: names ending in "rst" (not "ost" — "Horst" ends with "rst")
    pred_suffix: Like = Like("name", "%rst")
    assert pred_suffix(user_horst) is True  # "Horst" ends with "rst"
    assert pred_suffix(user_tom) is False
    assert pred_suffix(user_john) is False

    # Contains: names containing "or"
    pred_contains: Like = Like("name", "%or%")
    assert pred_contains(user_horst) is True  # "Horst" contains "or"
    assert pred_contains(user_tom) is False  # "Tom" does not contain "or"
    assert pred_contains(user_john) is False  # "John" does not contain "or"

    # Suffix that actually matches: "%ohn"
    pred_ohn: Like = Like("name", "%ohn")
    assert pred_ohn(user_john) is True  # "John" ends with "ohn"
    assert pred_ohn(user_horst) is False
    assert pred_ohn(user_tom) is False


# ---------------------------------------------------------------------------
# 8. In predicate
# ---------------------------------------------------------------------------


def test_in_predicate():
    """In('yob', [1972, 2003]) must match users whose yob is in the list."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: In = In("yob", [1972, 2003])

    assert pred(user_horst) is True  # 1972
    assert pred(user_tom) is False  # 1983
    assert pred(user_john) is True  # 2003


# ---------------------------------------------------------------------------
# 9. And composition
# ---------------------------------------------------------------------------


def test_and_composition():
    """And(Eq('department.name', 'Dev'), Gt('yob', 1980)) must match only
    users in the Dev department born after 1980 — i.e. only Tom."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: And = And(Eq("department.name", "Dev"), Gt("yob", 1980))

    assert pred(user_horst) is False  # Dev but yob=1972
    assert pred(user_tom) is True  # Dev and yob=1983
    assert pred(user_john) is False  # HR


# ---------------------------------------------------------------------------
# 10. Or composition
# ---------------------------------------------------------------------------


def test_or_composition():
    """Or(Eq('name', 'Horst'), Eq('name', 'Tom')) must match both Horst
    and Tom but not John."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Or = Or(Eq("name", "Horst"), Eq("name", "Tom"))

    assert pred(user_horst) is True
    assert pred(user_tom) is True
    assert pred(user_john) is False


# ---------------------------------------------------------------------------
# 11. Not predicate
# ---------------------------------------------------------------------------


def test_not_predicate():
    """Not(Eq('name', 'Horst')) must match everyone except Horst."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()
    pred: Not = Not(Eq("name", "Horst"))

    assert pred(user_horst) is False
    assert pred(user_tom) is True
    assert pred(user_john) is True


# ---------------------------------------------------------------------------
# 12. filter_values with structured predicate — end-to-end
# ---------------------------------------------------------------------------


def test_filter_values_with_structured_predicate():
    """A structured predicate used as filter_predicate in filter_values must
    produce the same result as an equivalent lambda. This confirms that
    structured predicates are true drop-in replacements."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # Structured predicate: filter Dev department users.
    pred: Eq = Eq("department.name", "Dev")
    filtered_struct: RF = filter_values[RF, RF](users, filter_predicate=pred).result

    # Equivalent lambda for comparison.
    filtered_lambda: RF = filter_values[RF, RF](
        users, filter_predicate=lambda v: v.department.name == "Dev"
    ).result

    # Both must yield the same set of user names.
    names_struct: set[str] = {item.value.name for item in filtered_struct}
    names_lambda: set[str] = {item.value.name for item in filtered_lambda}
    assert names_struct == names_lambda == {"Horst", "Tom"}
    assert len(filtered_struct) == 2


# ---------------------------------------------------------------------------
# 13. Plan IR — structured predicate is NOT Opaque
# ---------------------------------------------------------------------------


def test_plan_ir_structured_not_opaque():
    """When a structured predicate is used in filter_values, the plan IR
    must represent it as a Predicate object (not Opaque). This is the key
    advantage over lambdas: the plan is introspectable by backends."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    pred: Eq = Eq("department.name", "Dev")
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users, filter_predicate=pred
    )

    plan: LogicalPlan = pipeline.to_plan()
    root: PlanChild = plan.root
    assert isinstance(root, PlanNode)
    assert root.op == "filter_values"

    # The filter_predicate param must be the Predicate object itself,
    # NOT an Opaque marker.
    filter_param: Any = root.params["filter_predicate"]
    assert isinstance(filter_param, Eq)
    assert not isinstance(filter_param, Opaque)
    assert filter_param.attr == "department.name"
    assert filter_param.value == "Dev"


# ---------------------------------------------------------------------------
# 14. Plan IR — JSON roundtrip preserves structured predicates
# ---------------------------------------------------------------------------


def test_plan_ir_json_roundtrip():
    """A plan containing a structured predicate must survive to_json() ->
    from_json() and come back as the same Predicate type (not Opaque)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    pred: And = And(Eq("department.name", "Dev"), Gt("yob", 1980))
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users, filter_predicate=pred
    )

    plan: LogicalPlan = pipeline.to_plan()
    json_str: str = plan.to_json()

    # The JSON must contain the predicate structure.
    assert '"predicate"' in json_str
    assert '"eq"' in json_str
    assert '"gt"' in json_str

    # Round-trip back.
    restored: LogicalPlan = LogicalPlan.from_json(json_str)
    restored_root: PlanChild = restored.root
    assert isinstance(restored_root, PlanNode)

    filter_param: Any = restored_root.params["filter_predicate"]
    assert isinstance(filter_param, And)
    assert len(filter_param.predicates) == 2

    child_eq: Predicate = filter_param.predicates[0]
    child_gt: Predicate = filter_param.predicates[1]
    assert isinstance(child_eq, Eq)
    assert isinstance(child_gt, Gt)
    assert child_eq.attr == "department.name"
    assert child_eq.value == "Dev"
    assert child_gt.attr == "yob"
    assert child_gt.value == 1980


# ---------------------------------------------------------------------------
# 15. explain() shows predicate repr, not <opaque lambda>
# ---------------------------------------------------------------------------


def test_explain_shows_predicate():
    """When a structured predicate is used, explain() must show the
    predicate's repr (e.g. Eq('department.name', 'Dev')) instead of
    <opaque lambda>."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    pred: Eq = Eq("department.name", "Dev")
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users, filter_predicate=pred
    )

    explanation: str = pipeline.explain()
    assert "Eq(" in explanation
    assert "department.name" in explanation
    assert "<opaque lambda>" not in explanation


# ---------------------------------------------------------------------------
# 16. Lambda predicates still produce Opaque (backward compatibility)
# ---------------------------------------------------------------------------


def test_lambda_still_opaque():
    """Lambda predicates must still work and appear as Opaque in the plan IR.
    This confirms backward compatibility: the structured predicate path does
    not break the existing lambda path."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users, filter_predicate=lambda v: v.department.name == "Dev"
    )

    # Lambda still works for execution.
    result: RF = pipeline.result
    names: set[str] = {item.value.name for item in result}
    assert names == {"Horst", "Tom"}

    # In the plan, it must appear as Opaque.
    plan: LogicalPlan = pipeline.to_plan()
    root: PlanChild = plan.root
    assert isinstance(root, PlanNode)
    filter_param: Any = root.params["filter_predicate"]
    assert isinstance(filter_param, Opaque)
    assert filter_param.reason == "lambda"
    assert "<opaque lambda>" in pipeline.explain()


# ---------------------------------------------------------------------------
# 17. .where() with structured predicates
# ---------------------------------------------------------------------------


def test_where_with_structured_predicate():
    """Structured predicates passed to .where() are applied to item.value
    (filter_values semantics), producing the same result as the equivalent
    lambda or kwargs form."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # Structured predicate via .where():
    result_pred: RF = users.where(Eq("department.name", "Dev"))
    pred_names: set[str] = {item.value.name for item in result_pred}

    # Equivalent lambda via .where():
    result_lambda: RF = users.where(lambda i: i.value.department.name == "Dev")
    lambda_names: set[str] = {item.value.name for item in result_lambda}

    # Equivalent kwargs via .where():
    result_kwargs: RF = users.where(department__name="Dev")
    kwargs_names: set[str] = {item.value.name for item in result_kwargs}

    assert pred_names == {"Horst", "Tom"}
    assert pred_names == lambda_names
    assert pred_names == kwargs_names


def test_where_with_composed_predicate():
    """Composed predicates (And, Or, Not) work with .where()."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = users.where(And(Eq("department.name", "Dev"), Gt("yob", 1980)))
    names: set[str] = {item.value.name for item in result}
    assert names == {"Tom"}


# ---------------------------------------------------------------------------
# 18–25. Coverage: from_dict, repr, edge cases
# ---------------------------------------------------------------------------


def test_like_exact_and_wildcard_only():
    """Like with exact match (no %) and wildcard-only (%) pattern."""
    _, _, _, user_horst, _, _ = _make_users_rf()
    assert Like("name", "Horst")(user_horst) is True
    assert Like("name", "Tom")(user_horst) is False
    assert Like("name", "%")(user_horst) is True


def test_resolve_attr_path_empty_segment():
    """Pathological dunder paths with empty segments must raise ValueError."""
    from fql.predicates.predicates import _resolve_attr_path

    _, _, _, user_horst, _, _ = _make_users_rf()
    # Leading __ produces an empty first segment after normalization
    with pytest.raises(ValueError, match="Empty segment"):
        _resolve_attr_path(user_horst, "__name")


def test_predicate_from_dict_unknown_op():
    """Predicate.from_dict with an unknown op must raise ValueError."""
    with pytest.raises(ValueError, match="Unknown predicate op"):
        Predicate.from_dict({"type": "predicate", "op": "unknown_op"})


def test_ref_to_dict_from_dict_repr():
    """Ref serialization roundtrip and repr."""
    ref: Ref = Ref("department.name")
    d: dict = ref.to_dict()
    assert d == {"type": "ref", "attr": "department.name"}

    restored: Ref = Ref.from_dict(d)
    assert restored.attr == "department.name"
    assert repr(ref) == "Ref('department.name')"


def test_like_from_dict_repr():
    """Like serialization roundtrip and repr."""
    pred: Like = Like("name", "H%")
    d: dict = pred.to_dict()
    restored: Like = Like._from_dict(d)
    assert restored.attr == "name"
    assert restored.pattern == "H%"
    assert "Like(" in repr(pred)


def test_in_from_dict_repr():
    """In serialization roundtrip and repr."""
    pred: In = In("yob", [1972, 2003])
    d: dict = pred.to_dict()
    restored: In = In._from_dict(d)
    assert restored.attr == "yob"
    assert restored.values == [1972, 2003]
    assert "In(" in repr(pred)


def test_in_with_tuple_and_set():
    """In accepts tuple and set inputs; both coerce to list on roundtrip."""
    _, _, _, user_horst, user_tom, user_john = _make_users_rf()

    # tuple input
    pred_tuple: In = In("yob", (1972, 2003))
    assert pred_tuple(user_horst) is True
    assert pred_tuple(user_tom) is False
    assert pred_tuple(user_john) is True

    # set input
    pred_set: In = In("yob", {1972, 2003})
    assert pred_set(user_horst) is True
    assert pred_set(user_tom) is False

    # roundtrip coerces to list
    restored: In = In._from_dict(pred_tuple.to_dict())
    assert isinstance(restored.values, list)
    assert set(restored.values) == {1972, 2003}


def test_and_or_not_from_dict_repr():
    """And, Or, Not serialization roundtrip and repr."""
    and_pred: And = And(Eq("name", "A"), Eq("name", "B"))
    or_pred: Or = Or(Eq("name", "A"), Eq("name", "B"))
    not_pred: Not = Not(Eq("name", "A"))

    # And roundtrip
    and_restored: And = And._from_dict(and_pred.to_dict())
    assert len(and_restored.predicates) == 2
    assert "And(" in repr(and_pred)

    # Or roundtrip
    or_restored: Or = Or._from_dict(or_pred.to_dict())
    assert len(or_restored.predicates) == 2
    assert "Or(" in repr(or_pred)

    # Not roundtrip
    not_restored: Not = Not._from_dict(not_pred.to_dict())
    assert isinstance(not_restored.predicate, Eq)
    assert "Not(" in repr(not_pred)
