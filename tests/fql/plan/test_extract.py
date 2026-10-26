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

"""Tests for ``fql.plan.extract`` and ``fql.plan.ir``.

These tests verify that an un-executed FQL operator tree can be walked into a
``LogicalPlan`` without triggering computation, that lambdas surface as
``Opaque`` markers, that multi-level operator composition recurses correctly,
and that the IR survives a JSON round-trip.
"""

import re
from collections.abc import Mapping
from typing import Any, Callable

import pytest

from fdm.attribute_functions import RF, DBF
from fql.expressions import Attr, Composite, Expression, Key, Mul, Record
from fql.operators.APIs import Operator
from fql.operators.aggregates import (
    AggregationFunction,
    Avg,
    Count,
    Max,
    Mean,
    Median,
    Min,
    Sum,
    aggregate,
)
from fql.operators.constraints import drop_join_predicate
from fql.operators.filters import filter_items, filter_values
from fql.operators.partition import group_by, partition
from fql.operators.partition_and_aggregate import (
    group_by_aggregate,
    partition_by_aggregate,
)
from fql.operators.projections import project
from fql.operators.rank import rank_by
from fql.operators.set_operations import cogroup, intersect, minus, union
from fql.operators.subsets import subset
from fql.operators.transforms import transform_items
from fql.plan import LogicalPlan, LeafRef, Opaque, PlanNode, extract, extract_plan
from fql.plan.ir import IR_VERSION, PlanChild, _value_from_dict, _value_to_dict
from fql.predicates.predicates import Eq, Lt
from tests.lib import _create_testdata


def _users(frozen: bool = True) -> RF:
    db: DBF = _create_testdata(frozen=frozen)
    return db.users


# -- Basic leaf + single operator --------------------------------------------


def test_leaf_ref_from_attribute_function():
    """Extracting a bare ``AttributeFunction`` (here: an ``RF``) must produce
    a ``LeafRef`` that carries the AF's integer UUID, class name, and
    ``schema_name=None`` (no binding context), so that a backend can identify
    the leaf without ever seeing the AF object."""
    users: RF = _users()
    leaf: PlanChild = extract(users)
    assert isinstance(leaf, LeafRef)
    assert leaf.kind == "af"
    assert leaf.af_class == "RF"
    assert leaf.uuid == users.uuid
    assert isinstance(leaf.uuid, int)
    assert leaf.schema_name is None


def test_single_filter_values_extraction():
    """Happy path for a one-operator pipeline: a ``filter_values`` with a
    lambda predicate must extract into a ``LogicalPlan`` whose root is a
    ``PlanNode`` named after the operator class, whose single input is the
    underlying leaf, and whose ``filter_predicate`` parameter surfaces as an
    ``Opaque`` marker (v1 deliberately does not introspect lambdas)."""
    users: RF = _users()
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users,
        filter_predicate=lambda v: v.department.name == "Dev",
    )

    plan: LogicalPlan = pipeline.to_plan()

    assert isinstance(plan, LogicalPlan)
    assert plan.ir_version == IR_VERSION

    root: PlanChild = plan.root
    assert isinstance(root, PlanNode)
    # Note: filter_values subclasses filter_items and calls super().__init__()
    # wrapping the user predicate, so the operator class reported here is the
    # outer ``filter_values`` class name — that is the correct behaviour.
    assert root.op == "filter_values"

    # Exactly one source, which is the ``users`` leaf.
    assert len(root.inputs) == 1
    child: PlanChild = root.inputs[0]
    assert isinstance(child, LeafRef)
    assert child.uuid == users.uuid

    # The filter predicate must surface as Opaque — we do not introspect
    # Python lambdas in v1.
    assert "filter_predicate" in root.params
    pred_param: Opaque = root.params["filter_predicate"]
    assert isinstance(pred_param, Opaque)
    assert pred_param.reason == "lambda"
    assert pred_param.py_id != 0


def test_extraction_does_not_execute_the_operator():
    """``to_plan`` must not trigger ``_compute`` — the result cache stays empty."""
    users: RF = _users()
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users,
        filter_predicate=lambda v: v.department.name == "Dev",
    )
    # Sanity: no cached result yet.
    assert pipeline._result is None

    _: LogicalPlan = pipeline.to_plan()

    # Still no cached result after extraction.
    assert pipeline._result is None


# -- Nested composition ------------------------------------------------------


def test_nested_filter_chain_is_recursively_extracted():
    """A pipeline built by feeding one operator into another must extract
    into a corresponding chain of ``PlanNode``s bottoming out in a single
    ``LeafRef``. This guards the recursive descent through
    ``input_function`` and also reasserts the "no execution" invariant for
    multi-level pipelines."""
    users: RF = _users()

    inner: filter_items[RF, RF] = filter_items[RF, RF](
        users,
        filter_predicate=lambda i: i.value.department.name == "Dev",
    )
    outer: filter_items[RF, RF] = filter_items[RF, RF](
        inner,
        filter_predicate=lambda i: i.value.name == "Horst",
    )

    plan: LogicalPlan = outer.to_plan()
    root: PlanChild = plan.root

    assert isinstance(root, PlanNode)
    assert root.op == "filter_items"
    assert len(root.inputs) == 1

    mid: PlanChild = root.inputs[0]
    assert isinstance(mid, PlanNode)
    assert mid.op == "filter_items"
    assert len(mid.inputs) == 1

    leaf: PlanChild = mid.inputs[0]
    assert isinstance(leaf, LeafRef)
    assert leaf.uuid == users.uuid

    # Neither operator should have been executed.
    assert inner._result is None
    assert outer._result is None


# -- JSON round-trip ---------------------------------------------------------


def test_json_roundtrip_preserves_structure():
    """A ``LogicalPlan`` must survive ``to_json`` → ``from_json`` with its
    operator identity, input structure, leaf identity, and ``Opaque``
    parameter markers (including ``reason``, ``repr``, ``py_id``) all
    intact. This is the contract that lets a plan cross a process boundary."""
    users: RF = _users()
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users,
        filter_predicate=lambda v: v.department.name == "Dev",
    )

    plan: LogicalPlan = pipeline.to_plan()
    json_str: str = plan.to_json()
    assert isinstance(json_str, str)
    assert '"ir_version"' in json_str
    assert '"filter_values"' in json_str

    restored: LogicalPlan = LogicalPlan.from_json(json_str)
    assert restored.ir_version == plan.ir_version

    # Structural equality after roundtrip.
    original_root: PlanChild = plan.root
    restored_root: PlanChild = restored.root
    assert isinstance(restored_root, PlanNode)
    assert isinstance(original_root, PlanNode)
    assert restored_root.op == original_root.op
    assert len(restored_root.inputs) == len(original_root.inputs)

    restored_leaf: PlanChild = restored_root.inputs[0]
    assert isinstance(restored_leaf, LeafRef)
    assert restored_leaf.uuid == users.uuid
    assert restored_leaf.af_class == "RF"

    # Opaque predicate survives with reason, repr, and py_id intact.
    original_pred: Opaque = original_root.params["filter_predicate"]
    restored_pred: Opaque = restored_root.params["filter_predicate"]
    assert isinstance(original_pred, Opaque)
    assert isinstance(restored_pred, Opaque)
    assert restored_pred.reason == original_pred.reason == "lambda"
    assert restored_pred.repr == original_pred.repr
    assert restored_pred.py_id == original_pred.py_id


# -- explain() ---------------------------------------------------------------


def test_explain_contains_operator_and_leaf_info():
    """The human-readable ``explain()`` pretty-printer must mention the
    operator class name, the leaf AF class, and clearly mark opaque
    parameters so a developer reading it can spot non-serializable pieces
    of the plan at a glance."""
    users: RF = _users()
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users,
        filter_predicate=lambda v: True,
    )
    text: str = pipeline.to_plan().explain()
    assert "filter_values" in text
    assert "RF" in text
    assert "<opaque lambda>" in text


# -- IR version handling -----------------------------------------------------


def test_from_dict_rejects_wrong_ir_version():
    """``LogicalPlan.from_dict`` must refuse plans tagged with a foreign IR
    version rather than silently misinterpret them."""
    good: dict = extract_plan(_users()).to_dict()
    bad: dict = dict(good)
    bad["ir_version"] = IR_VERSION + 99  # future / unknown version

    with pytest.raises(ValueError, match="Unsupported IR version"):
        LogicalPlan.from_dict(bad)


def test_from_dict_missing_ir_version_defaults_to_current():
    """When ``ir_version`` is absent we assume the current version — this
    keeps hand-written test fixtures ergonomic while still rejecting
    *explicit* mismatches."""
    plan_dict: dict = extract_plan(_users()).to_dict()
    plan_dict.pop("ir_version")
    restored: LogicalPlan = LogicalPlan.from_dict(plan_dict)
    assert restored.ir_version == IR_VERSION


# -- _value_to_dict / _value_from_dict coverage ------------------------------


def test_value_roundtrip_primitives():
    """Primitives and ``None`` must pass through ``_value_to_dict`` /
    ``_value_from_dict`` unchanged."""
    for v in (None, "s", 1, 1.5, True, False):
        assert _value_from_dict(_value_to_dict(v)) == v


def test_value_roundtrip_nested_dict_and_list():
    """Nested dicts and list/tuple values should roundtrip recursively.
    Note: tuples are normalized to lists on the wire, matching JSON."""
    value: dict = {
        "a": [1, 2, 3],
        "b": {"inner": ("x", "y")},
        "c": None,
    }
    rt: Any = _value_from_dict(_value_to_dict(value))
    assert rt == {
        "a": [1, 2, 3],
        "b": {"inner": ["x", "y"]},
        "c": None,
    }


def test_value_to_dict_coerces_unknown_types_to_opaque():
    """Anything outside the supported JSON shapes (e.g. ``complex``) must be
    coerced to an ``Opaque`` dict rather than crashing serialization."""
    encoded: Any = _value_to_dict(complex(1, 2))  # complex number
    assert isinstance(encoded, dict)
    assert encoded["type"] == "opaque"
    assert encoded["reason"] == "unknown"
    assert "1" in encoded["repr"] and "2" in encoded["repr"]


# -- Non-lambda callables ----------------------------------------------------


def _module_level_predicate(v: Any) -> bool:
    """Module-level function used as a predicate — must surface as Opaque
    with reason ``'callable'`` (not ``'lambda'``)."""
    return True


class _CallablePredicate:
    """Callable class used as a predicate."""

    def __call__(self, v: Any) -> bool:
        return True


@pytest.mark.parametrize(
    "predicate, expected_repr_fragment",
    [
        (_module_level_predicate, "_module_level_predicate"),
        (_CallablePredicate(), "_CallablePredicate"),
    ],
    ids=["def-function", "callable-class"],
)
def test_non_lambda_callable_becomes_opaque_callable(
    predicate: Callable, expected_repr_fragment: str
):
    """Non-lambda callables (``def``-functions and instances with
    ``__call__``) must be reported as ``Opaque`` with
    ``reason='callable'``, not ``'lambda'``. We use ``filter_items`` here
    because ``filter_values`` internally re-wraps the user predicate in
    its own lambda."""
    users: RF = _users()
    pipeline: filter_items[RF, RF] = filter_items[RF, RF](
        users, filter_predicate=predicate
    )
    root: PlanChild = pipeline.to_plan().root
    assert isinstance(root, PlanNode)
    pred: Opaque = root.params["filter_predicate"]
    assert isinstance(pred, Opaque)
    assert pred.reason == "callable"
    assert expected_repr_fragment in pred.repr


# -- DBF extraction -----------------------------------------------------------


def test_extract_dbf_produces_dbf_bind():
    """Extracting a ``DBF`` must produce a ``DBF_bind`` node whose
    ``params['names']`` carries the relation names and whose children are
    the extracted sub-plans (``LeafRef``s for bare RFs) in order. Binding
    names are stamped onto leaf children as ``schema_name``."""
    db: DBF = _create_testdata(frozen=True)

    root: PlanChild = extract(db)
    assert isinstance(root, PlanNode)
    assert root.op == "DBF_bind"
    # The testdata DBF has three relations: departments, users, customers.
    names: list[str] = list(root.params["names"])
    assert "users" in names
    assert "departments" in names
    assert "customers" in names
    assert len(root.inputs) == len(names)

    # Each source is a LeafRef with the binding name stamped on.
    for i, name in enumerate(names):
        child: PlanChild = root.inputs[i]
        assert isinstance(child, LeafRef)
        assert child.schema_name == name
        assert child.af_class == "RF"


def test_extract_dbf_with_operator_input():
    """When an operator's ``input_function`` is a ``DBF``, the extractor
    must produce a ``PlanNode`` whose source is a ``DBF_bind`` — not a flat
    ``LeafRef``. This preserves the named structure that a backend needs
    to resolve joins implied by foreign-object constraints."""
    db: DBF = _create_testdata(frozen=True)
    pipeline: filter_items[DBF, DBF] = filter_items[DBF, DBF](
        db,
        filter_predicate=lambda i: i.key != "customers",
    )

    plan: LogicalPlan = pipeline.to_plan()
    root: PlanChild = plan.root
    assert isinstance(root, PlanNode)
    assert root.op == "filter_items"

    # The source must be a DBF_bind, not a LeafRef.
    child: PlanChild = root.inputs[0]
    assert isinstance(child, PlanNode)
    assert child.op == "DBF_bind"
    names: list[str] = list(child.params["names"])
    assert "users" in names
    assert "departments" in names


# -- extract() edge cases (bare dict, literal fallback, param serialization) --


def test_extract_bare_dict_still_produces_dbf_bind():
    """A bare Python dict (not a ``DBF``) passed to ``extract`` must still
    produce a ``DBF_bind`` node for robustness, even though the idiomatic
    FDM way is to use a ``DBF``."""
    db: DBF = _create_testdata(frozen=True)
    bare: dict[str, RF] = {"users": db.users, "departments": db.departments}

    root: PlanChild = extract(bare)
    assert isinstance(root, PlanNode)
    assert root.op == "DBF_bind"
    assert list(root.params["names"]) == ["users", "departments"]


def test_extract_literal_fallback_for_unsupported_types():
    """Passing an unsupported type (e.g. a plain integer) to ``extract``
    must produce a synthetic ``literal`` node rather than crashing."""
    root: PlanChild = extract(42)
    assert isinstance(root, PlanNode)
    assert root.op == "literal"
    assert root.params["value"] == 42


def test_serialize_param_hides_private_and_passes_none():
    """Private operator attributes are hidden; ``None`` params pass through as ``None``.
    Would fail if the private ``_create_lineage`` flag (or any other ``_``-prefixed
    attribute) leaked into the plan, or if ``None`` were turned into an ``Opaque``."""
    db: DBF = _create_testdata(frozen=True)  # departments, users, customers
    pipeline: filter_items[RF, RF] = filter_items[RF, RF](
        db.users, filter_predicate=lambda i: True
    )  # filter_items stores the private flag _create_lineage
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert "_create_lineage" not in root.params  # private flag is not a plan param
    assert not any(
        k.startswith("_") for k in root.params
    )  # no private attribute at all

    dropper: drop_join_predicate[DBF, DBF] = drop_join_predicate[DBF, DBF](
        db, description="x"
    )  # description mode: predicate and matcher stay None
    drop_root: PlanChild = dropper.to_plan().root  # extract without executing
    assert isinstance(drop_root, PlanNode)  # an operator node, not a leaf
    assert drop_root.params["description"] == "x"  # the string param is kept as is
    assert drop_root.params["predicate"] is None  # None passes through unchanged
    assert drop_root.params["matcher"] is None  # None passes through unchanged


# -- Pretty-print nested plan ------------------------------------------------


def test_explain_nested_plan_contains_both_levels():
    """``explain()`` on a two-level pipeline must indent and render every
    level of the plan, with monotonically increasing indentation from the
    outermost operator down to the leaf."""
    users: RF = _users()
    inner: filter_items[RF, RF] = filter_items[RF, RF](
        users, filter_predicate=lambda i: True
    )
    outer: filter_values[RF, RF] = filter_values[RF, RF](
        inner, filter_predicate=lambda v: True
    )

    text: str = outer.to_plan().explain()
    lines: list[str] = text.splitlines()

    # Both operator levels must appear.
    assert any("filter_values" in line for line in lines)
    assert any("filter_items" in line for line in lines)
    # And a leaf line for the underlying users RF.
    assert any("leaf RF" in line for line in lines)

    # Indentation must be monotonically increasing down the tree.
    # Find the line index of each level in the explain output.

    # Find the outermost operator (filter_values wraps filter_items).
    outer_idx: int = next(i for i, l in enumerate(lines) if "filter_values" in l)
    # Find the inner operator (filter_items sits one level deeper).
    inner_idx: int = next(i for i, l in enumerate(lines) if "filter_items" in l)
    # Find the leaf node (the underlying users RF at the bottom of the tree).
    leaf_idx: int = next(i for i, l in enumerate(lines) if "leaf RF" in l)

    # Measure leading whitespace (= indentation depth) for each level.
    # len(line) - len(line.lstrip()) counts the number of leading spaces.
    outer_indent: int = len(lines[outer_idx]) - len(lines[outer_idx].lstrip())
    inner_indent: int = len(lines[inner_idx]) - len(lines[inner_idx].lstrip())
    leaf_indent: int = len(lines[leaf_idx]) - len(lines[leaf_idx].lstrip())

    # Outer operator < inner operator < leaf: tree structure reflected visually.
    assert outer_indent < inner_indent < leaf_indent


# -- Structured params instead of accidental Opaque markers (006) -------------


def _find_opaques(value: Any) -> list[Opaque]:
    """Collect every ``Opaque`` marker found anywhere below ``value``.

    Walks ``PlanNode`` params and inputs as well as nested dicts, lists and
    tuples recursively. Leaves, primitives, structured predicates and
    aggregation functions contain no markers and contribute nothing.
    """
    if isinstance(value, Opaque):
        return [value]
    if isinstance(value, PlanNode):
        return _find_opaques(value.params) + _find_opaques(value.inputs)
    if isinstance(value, Mapping):
        return [opaque for v in value.values() for opaque in _find_opaques(v)]
    if isinstance(value, (list, tuple)):
        return [opaque for v in value for opaque in _find_opaques(v)]
    return []


def test_project_plan_shows_attribute_names():
    """``project`` exposes its attribute names in the plan, without any ``Opaque``.
    Would fail if the internal ``af.project(...)`` lambda leaked into the plan
    as ``transformation_function``."""
    users: RF = _users()  # frozen users relation: name, yob, department
    pipeline: project = project(users, "name", "yob")  # keep name and yob only
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.op == "project"  # named after the operator class
    assert root.params["attributes"] == ["name", "yob"]  # tuple -> list, order kept
    assert "transformation_function" not in root.params  # internal lambda hidden
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_group_by_plan_shows_grouping_keys():
    """``group_by`` exposes its grouping keys in the plan, without any ``Opaque``.
    Would fail if the derived partitioning key (``Attr`` / ``Composite``) or the
    default output factory showed up in the plan."""
    users: RF = _users()  # frozen users relation
    pipeline: group_by = group_by(users, "department")  # one group per department
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.params["aggregate_keys"] == ["department"]  # grouping key visible
    assert (
        "partitioning_function" not in root.params
    )  # derived key (Attr/Composite) hidden
    assert "output_factory" not in root.params  # default factory lambda hidden
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_aggregate_plan_survives_json_roundtrip():
    """Built-in aggregations are structured in the plan and survive a JSON round trip.
    Would fail if ``Count``/``Sum`` became ``Opaque`` or came back as plain dicts
    instead of aggregation objects."""
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(users, n=Count(), s=Sum("yob"))  # count, sum
    plan: LogicalPlan = pipeline.to_plan()  # extract without executing
    assert _find_opaques(plan.root) == []  # built-in aggregations are not opaque

    restored: LogicalPlan = LogicalPlan.from_json(plan.to_json())  # JSON round trip
    assert isinstance(restored.root, PlanNode)  # still an operator node
    n: AggregationFunction = restored.root.params["aggregates"]["n"]  # restored Count
    s: AggregationFunction = restored.root.params["aggregates"]["s"]  # restored Sum
    assert type(n) is Count  # Count comes back as exactly Count
    assert n.to_dict() == pipeline.aggregates["n"].to_dict()  # same op, no attr
    assert type(s) is Sum  # Sum comes back as exactly Sum
    assert s.to_dict() == pipeline.aggregates["s"].to_dict()  # same op, attr "yob"


def test_group_by_aggregate_plan_has_no_opaques():
    """``group_by_aggregate`` with a built-in aggregation has no ``Opaque`` in its plan.
    Would fail if an aggregation nested in the ``aggregates`` dict were not
    recognised as serializable."""
    users: RF = _users()  # frozen users relation
    pipeline: group_by_aggregate = group_by_aggregate(
        users, "department", n=Count()
    )  # number of users per department
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.params["aggregate_keys"] == ["department"]  # grouping key visible
    assert type(root.params["aggregates"]["n"]) is Count  # aggregation kept, structured
    assert root.params["aggregates"]["n"].to_dict() == {
        "type": "aggregation",
        "op": "count",
        "attr": None,
    }  # literal IR record: Count without attribute counts all rows
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_partition_by_aggregate_keyword_plan_has_no_opaques():
    """Keyword aggregates and a predicate key give a plan without any ``Opaque``.
    Would fail if the keyword aggregates were hidden in an internal lambda."""
    users: RF = _users()  # frozen users relation
    pipeline: partition_by_aggregate = partition_by_aggregate(
        users, partitioning_function=Lt("yob", 1980), n=Count()
    )  # number of users born before 1980 vs. the rest
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.params["aggregation_function"] is None  # lambda form unused
    assert type(root.params["aggregates"]["n"]) is Count  # aggregation kept, structured
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_partition_by_aggregate_lambda_plan_keeps_aggregation_opaque():
    """The lambda form shows its aggregation as ``Opaque`` and no keyword aggregates.
    Would fail if the lambda were dropped from the plan, or if the unused
    ``aggregates`` param showed up as anything other than an empty dict."""
    users: RF = _users()  # frozen users relation
    pipeline: partition_by_aggregate = partition_by_aggregate(
        users,
        partitioning_function=Attr("department"),
        aggregation_function=lambda i: i,
    )  # lambda form: the aggregation is an arbitrary callable
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.params["aggregates"] == {}  # keyword form unused
    assert isinstance(
        root.params["aggregation_function"], Opaque
    )  # lambda cannot be introspected


@pytest.mark.parametrize("set_operation", [union, intersect, minus])
def test_set_operation_plan_omits_output_factory(set_operation: type) -> None:
    """Set operations leave their mandatory output factory out of the plan.
    Would fail if ``Operator._plan_params`` stopped omitting ``output_factory``,
    which would put an ``Opaque`` lambda into every set-operation plan."""
    db: DBF = _create_testdata(frozen=True)  # departments, users, customers
    pipeline: Any = set_operation(db, output_factory=lambda _: RF())  # lambda factory
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert "output_factory" not in root.params  # the factory lambda is omitted
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_cogroup_plan_omits_all_output_factories():
    """``cogroup`` leaves all three output factories out of the plan.
    Would fail if ``output_factory_nested`` or the defaulted
    ``output_factory_leaf`` lambda leaked into the plan as ``Opaque``."""
    db: DBF = _create_testdata(frozen=True)  # departments, users, customers
    pipeline: cogroup = cogroup(
        db, output_factory=lambda _: RF(), output_factory_nested=lambda _: RF()
    )  # output_factory_leaf stays at its lambda default
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert "output_factory" not in root.params  # outer factory omitted
    assert "output_factory_nested" not in root.params  # nested factory omitted
    assert "output_factory_leaf" not in root.params  # default leaf factory omitted
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_partition_plan_omits_default_output_factory():
    """``partition`` omits its default factory; only the user's own lambda stays opaque.
    Would fail if the default ``DBF`` factory lambda showed up as a second
    ``Opaque`` marker."""
    users: RF = _users()  # frozen users relation
    pipeline: partition = partition(
        users, partitioning_function=lambda i: i.value.yob < 1980
    )  # genuine user lambda; output_factory left at its default
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert "output_factory" not in root.params  # the default factory is omitted
    opaques: list[Opaque] = _find_opaques(root)  # all remaining opaque markers
    assert len(opaques) == 1  # exactly one marker is left ...
    assert opaques[0] is root.params["partitioning_function"]  # ... the user lambda


def test_filter_values_with_predicate_and_factory_has_no_opaques():
    """A structured predicate plus an explicit factory gives a plan without ``Opaque``.
    Would fail if an explicitly passed ``output_factory`` (not just the default)
    were still serialized into the plan."""
    users: RF = _users()  # frozen users relation
    predicate: Eq = Eq("name", "Tom")  # structured predicate, kept for comparison
    pipeline: filter_values[RF, RF] = filter_values[RF, RF](
        users, predicate, output_factory=lambda _: RF()
    )  # structured predicate plus a factory lambda
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert "output_factory" not in root.params  # the factory lambda is omitted
    plan_predicate: Any = root.params["filter_predicate"]  # predicate in the plan
    assert plan_predicate.to_dict() == predicate.to_dict()  # same structured predicate
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan


def test_explain_shows_readable_aggregations():
    """``explain()`` renders built-in aggregations readably, e.g. ``Count()``.
    Would fail if ``AggregationFunction.__repr__`` fell back to the default
    ``<... object at 0x...>`` form."""
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(users, n=Count(), s=Sum("yob"))  # count, sum
    text: str = pipeline.to_plan().explain()  # human-readable plan
    assert "Count()" in text  # Count without attribute
    assert "Sum('yob')" in text  # Sum with its attribute
    assert "0x" not in text  # no memory address anywhere


# -- Aggregation (de)serialization: edge cases and failure modes (006) --------


@pytest.mark.parametrize(
    "aggregation, expected",
    [
        (Max("a"), {"type": "aggregation", "op": "max", "attr": "a"}),
        (Min("a"), {"type": "aggregation", "op": "min", "attr": "a"}),
        (Count(), {"type": "aggregation", "op": "count", "attr": None}),
        (Count("a"), {"type": "aggregation", "op": "count", "attr": "a"}),
        (Sum("a"), {"type": "aggregation", "op": "sum", "attr": "a"}),
        (Avg("a"), {"type": "aggregation", "op": "avg", "attr": "a"}),
        (Median("a"), {"type": "aggregation", "op": "median", "attr": "a"}),
    ],
    ids=["max", "min", "count-star", "count-attr", "sum", "avg", "median"],
)
def test_builtin_aggregation_dict_form_is_pinned(
    aggregation: AggregationFunction, expected: dict
) -> None:
    """Each built-in aggregation serializes to a fixed dict and restores as its class.
    Would fail if an ``op_name`` were renamed (breaking stored plans) or the
    ``_AGGREGATIONS_BY_OP`` registry mapped an op to the wrong class."""
    assert aggregation.to_dict() == expected  # exact IR record, literal values
    restored: AggregationFunction = AggregationFunction.from_dict(expected)  # restore
    assert type(restored) is type(aggregation)  # exactly the same built-in class
    assert restored.attribute == expected["attr"]  # attribute carried over


def test_mean_serializes_as_avg_and_restores_as_avg():
    """``Mean`` is a synonym: it serializes as ``"avg"`` and comes back as ``Avg``.
    Would fail if ``Mean`` got its own op name or if ``"avg"`` restored as ``Mean``."""
    data: dict = Mean("a").to_dict()  # serialize the synonym
    assert data["op"] == "avg"  # same op name as Avg
    assert type(AggregationFunction.from_dict(data)) is Avg  # restored as exactly Avg


@pytest.mark.parametrize(
    "data, message",
    [
        (
            {"type": "aggregation", "op": "stddev", "attr": "a"},
            "Unknown aggregation op",
        ),
        ({"type": "aggregation", "attr": "a"}, "Unknown aggregation op"),
        ({"type": "aggregation", "op": 3, "attr": "a"}, "Unknown aggregation op"),
        ({"type": "aggregation", "op": "sum", "attr": None}, "requires an attribute"),
        ({"type": "aggregation", "op": "sum", "attr": 5}, "must be a string"),
    ],
    ids=["unknown-op", "missing-op", "non-string-op", "sum-without-attr", "int-attr"],
)
def test_aggregation_from_dict_rejects_malformed_records(
    data: dict, message: str
) -> None:
    """``from_dict`` rejects malformed aggregation records with a ``ValueError``.
    Would fail if a bad record crashed with ``KeyError``/``TypeError`` or silently
    produced an aggregation that only fails later at execution time."""
    with pytest.raises(ValueError, match=message):  # clear error, right reason
        AggregationFunction.from_dict(data)  # deserialize the malformed record


def test_count_from_dict_without_attr_key_restores_count_star():
    """A ``count`` record without an ``"attr"`` key restores as ``Count()``.
    Would fail if ``from_dict`` required the ``"attr"`` key (``KeyError``) or
    restored a different class or a non-``None`` attribute."""
    data: dict = {"type": "aggregation", "op": "count"}  # "attr" key missing
    restored: AggregationFunction = AggregationFunction.from_dict(data)  # restore
    assert type(restored) is Count  # exactly Count, no subclass
    assert restored.attribute is None  # counts all tuples (COUNT(*))
    assert repr(restored) == "Count()"  # readable form without attribute


@pytest.mark.parametrize(
    "aggregation",
    [Sum(None), Max(5), Count(5)],
    ids=["sum-without-attr", "max-int-attr", "count-int-attr"],
)
def test_builtin_aggregation_with_invalid_attribute_is_not_serializable(
    aggregation: AggregationFunction,
) -> None:
    """A built-in aggregation with an invalid attribute is not serializable and stays opaque.
    Would fail if ``is_serializable`` checked only the class, so that a plan were
    written which ``from_dict`` later rejects (no attribute / non-string)."""
    assert aggregation.is_serializable() is False  # invalid attribute -> not in IR
    with pytest.raises(ValueError, match="is not serializable"):  # refused
        aggregation.to_dict()  # attempt to serialize
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(users, x=aggregation)  # build only, no execution
    root: PlanChild = pipeline.to_plan().root  # extraction must not crash
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    marker: Any = root.params["aggregates"]["x"]  # the serialized aggregation
    assert isinstance(marker, Opaque)  # falls back to an opaque marker
    assert _find_opaques(root) == [marker]  # and it is the only one in the plan
    restored: LogicalPlan = LogicalPlan.from_json(
        pipeline.to_plan().to_json()
    )  # JSON round trip must not raise (no from_dict on an invalid record)
    assert isinstance(restored.root, PlanNode)  # still an operator node
    assert isinstance(
        restored.root.params["aggregates"]["x"], Opaque
    )  # comes back as an opaque marker, not as a broken aggregation


def test_count_with_or_without_attribute_stays_serializable():
    """``Count()`` and ``Count("yob")`` remain serializable under the attribute rule.
    Would fail if the attribute check in ``is_serializable`` rejected ``None`` for
    ``Count`` too, turning every ``Count()`` in a plan into an ``Opaque`` marker."""
    assert Count().is_serializable() is True  # no attribute is fine for Count
    assert Count("yob").is_serializable() is True  # string attribute is fine


class _MyAvg(Avg):
    """User subclass of a built-in aggregation; it may change the behaviour."""

    def __call__(self, rf: RF) -> Any:
        """Return a constant instead of the average (behaviour differs from Avg)."""
        return 0


@pytest.mark.parametrize(
    "aggregation",
    [AggregationFunction(len, "a"), _MyAvg("a")],
    ids=["custom-function", "user-subclass"],
)
def test_aggregation_to_dict_rejects_non_builtin(
    aggregation: AggregationFunction,
) -> None:
    """``to_dict`` refuses aggregations whose behaviour is not described by op + attr.
    Would fail if a user subclass serialized under its parent's op name, so that
    a backend would silently compute the parent's aggregate instead."""
    with pytest.raises(ValueError, match="is not serializable"):  # refused
        aggregation.to_dict()  # attempt to serialize


def _my_aggregation_fn(values: list) -> int:
    """Module-level custom aggregation: number of distinct values."""
    return len(set(values))


def test_custom_aggregation_function_becomes_opaque_with_readable_repr():
    """``AggregationFunction(custom_fn, attr)`` is ``Opaque`` and names the function.
    Would fail if a custom aggregation were structured (its op is unknown) or if
    the marker showed only a memory address instead of the function name."""
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(
        users, x=AggregationFunction(_my_aggregation_fn, "yob")
    )  # user-built aggregation over yob
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    marker: Any = root.params["aggregates"]["x"]  # the serialized aggregation
    assert isinstance(marker, Opaque)  # custom aggregation is opaque
    assert "_my_aggregation_fn" in marker.repr  # names the wrapped function ...
    assert "'yob'" in marker.repr  # ... and the attribute


class _MySum(Sum):
    """User subclass of ``Sum`` with its own behaviour."""

    def __call__(self, rf: RF) -> Any:
        """Sum the attribute twice (behaviour differs from Sum)."""
        return 2 * super().__call__(rf)


def test_user_subclass_of_builtin_aggregation_becomes_opaque():
    """A user subclass of ``Sum`` is ``Opaque`` in the plan, not structured as ``sum``.
    Would fail if ``is_serializable`` used ``isinstance`` instead of the exact
    type, letting a backend run a plain SUM instead of the user's code."""
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(users, s=_MySum("yob"))  # overridden __call__
    root: PlanChild = pipeline.to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    marker: Any = root.params["aggregates"]["s"]  # the serialized aggregation
    assert isinstance(marker, Opaque)  # not structured as "sum"
    assert marker.reason == "callable"  # an instance with __call__, not a lambda
    assert "_MySum" in marker.repr  # the user class name is shown


class _Broken(AggregationFunction):
    """User subclass that skips ``super().__init__``: no attributes are set."""

    def __init__(self) -> None:
        """Deliberately do not call ``super().__init__``."""
        pass

    def __call__(self, rf: RF) -> Any:
        """Count the tuples."""
        return len(rf)


def test_aggregation_without_super_init_has_repr_and_extracts_as_opaque():
    """A subclass skipping ``super().__init__`` has a ``repr`` and extracts as Opaque.
    Would fail if ``__repr__`` read ``self.attribute`` directly (AttributeError),
    which would crash ``explain()`` and the whole plan extraction."""
    assert repr(_Broken()) == "_Broken()"  # no attributes -> empty argument list
    users: RF = _users()  # frozen users relation
    pipeline: aggregate = aggregate(users, b=_Broken())  # build only
    root: PlanChild = pipeline.to_plan().root  # extraction must not crash
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    marker: Any = root.params["aggregates"]["b"]  # the serialized aggregation
    assert isinstance(marker, Opaque)  # user aggregation is opaque
    assert marker.repr == "_Broken()"  # readable repr recorded in the marker


class _MyCountDistinct(AggregationFunction):
    """Direct user subclass of ``AggregationFunction`` that wraps its own function."""

    def __init__(self, attribute: str) -> None:
        """Wrap ``_my_aggregation_fn`` over ``attribute``."""
        super().__init__(_my_aggregation_fn, attribute)


@pytest.mark.parametrize(
    "aggregation, expected",
    [
        (_MyAvg("a"), "_MyAvg('a')"),
        (_MyCountDistinct("a"), "_MyCountDistinct(_my_aggregation_fn, 'a')"),
        (
            AggregationFunction(_my_aggregation_fn, "a"),
            "AggregationFunction(_my_aggregation_fn, 'a')",
        ),
    ],
    ids=["builtin-subclass", "direct-subclass", "plain-aggregation-function"],
)
def test_aggregation_repr_shows_wrapped_function_only_for_non_builtins(
    aggregation: AggregationFunction, expected: str
) -> None:
    """The ``repr`` names the wrapped function only outside built-in aggregations.
    Would fail if a built-in subclass showed ``Avg``'s placeholder ``len`` (misleading)
    or if a user-wrapped function were hidden from the ``Opaque`` marker."""
    assert repr(aggregation) == expected  # exact readable form, class name first


# -- IR version and JSON round trip of structured params (006) ---------------


@pytest.mark.parametrize("old_version", [1, 2, 3])
def test_from_dict_rejects_old_ir_version_plans(old_version: int) -> None:
    """Plans written with an older IR version (1, 2 or 3) are rejected with a clear error.
    Would fail if ``IR_VERSION`` had not been raised with each format change, so an
    old plan (plain-dict aggregations, lambda-only ranking keys, or predicate keys
    evaluated on the item) would be read as if it were current."""
    plan_dict: dict = extract_plan(_users()).to_dict()  # a valid current plan
    plan_dict["ir_version"] = old_version  # pretend an older version wrote it
    expected: str = (
        f"Unsupported IR version: {old_version} (expected {IR_VERSION})"  # message
    )
    with pytest.raises(ValueError, match=re.escape(expected)):  # rejected
        LogicalPlan.from_dict(plan_dict)  # read the old plan


def test_project_and_group_by_keys_survive_json_roundtrip():
    """``project`` attributes and ``group_by`` keys come back as plain lists from JSON.
    Would fail if these new structured params were dropped or turned into
    ``Opaque`` markers by the IR (de)serialization."""
    users: RF = _users()  # frozen users relation
    projected: project = project(users, "name", "department")  # two attributes
    grouped: group_by = group_by(projected, "department")  # group the projection
    restored: LogicalPlan = LogicalPlan.from_json(grouped.to_plan().to_json())  # JSON
    root: PlanChild = restored.root  # the group_by node
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert root.params["aggregate_keys"] == ["department"]  # grouping key restored
    inner: PlanChild = root.inputs[0]  # the project node below it
    assert isinstance(inner, PlanNode)  # an operator node, not a leaf
    assert inner.params["attributes"] == ["name", "department"]  # restored


# -- Structured ranking keys Attr / Key (007) --------------------------------

#: Ranking pipelines with a structured key and the IR dict that key must produce.
_RANKING_CASES: list = [
    pytest.param(
        lambda users: rank_by(users, ranking_key=Attr("yob")),
        {"type": "expression", "op": "attr", "attr": "yob"},
        id="rank_by-attr",
    ),
    pytest.param(
        lambda users: rank_by(users, ranking_key=Key(), reverse=True),
        {"type": "expression", "op": "key"},
        id="rank_by-key",
    ),
    pytest.param(
        lambda users: subset(users, ranking_key=Attr("yob"), k=2),
        {"type": "expression", "op": "attr", "attr": "yob"},
        id="subset-attr",
    ),
    pytest.param(
        lambda users: subset(users, ranking_key=Key(), k=2, reverse=True),
        {"type": "expression", "op": "key"},
        id="subset-key",
    ),
]


@pytest.mark.parametrize("build, expected", _RANKING_CASES)
def test_structured_ranking_key_extracts_without_opaque(
    build: Callable[[RF], Operator], expected: dict
) -> None:
    """``rank_by`` / ``subset`` with ``Attr`` or ``Key`` extract without ``Opaque``.
    Would fail if ``_serialize_param`` turned the callable expression into an
    ``Opaque`` marker, or if ``to_dict`` wrote a different IR record."""
    root: PlanChild = build(_users()).to_plan().root  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan
    assert root.params["ranking_key"].to_dict() == expected  # exact IR record


@pytest.mark.parametrize("build, expected", _RANKING_CASES)
def test_structured_ranking_key_survives_json_roundtrip(
    build: Callable[[RF], Operator], expected: dict
) -> None:
    """A JSON round trip restores exactly the ``Attr`` / ``Key`` ranking key.
    Would fail if ``_value_from_dict`` did not handle ``"type": "expression"``
    and returned a plain dict, or restored a different expression class."""
    pipeline: Operator = build(_users())  # ranking pipeline, not executed
    original: Expression = pipeline.ranking_key  # the key passed by the user
    restored: LogicalPlan = LogicalPlan.from_json(
        pipeline.to_plan().to_json()
    )  # serialize to JSON text and read it back
    root: PlanChild = restored.root  # the rank_by / subset node
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    key: Any = root.params["ranking_key"]  # the restored ranking key
    assert type(key) is type(original)  # exactly Attr or Key, no dict
    assert key.to_dict() == expected  # same attribute path (for Attr)
    assert root.params["reverse"] is pipeline.reverse  # sort direction restored


def test_explain_shows_readable_ranking_keys():
    """``explain()`` shows structured ranking keys as ``Attr('yob')`` and ``Key()``.
    Would fail if ``Attr.__repr__`` / ``Key.__repr__`` fell back to the default
    ``<... object at 0x...>`` form or the keys were shown as opaque markers."""
    ranked: rank_by = rank_by(_users(), ranking_key=Attr("yob"))  # rank by age
    top_2: subset = subset(ranked, ranking_key=Key(), k=2)  # first two ranks
    text: str = top_2.to_plan().explain()  # human-readable plan of both operators
    assert "Attr('yob')" in text  # rank_by key, readable
    assert "Key()" in text  # subset key, readable
    assert "<opaque" not in text  # no opaque marker anywhere
    assert "0x" not in text  # no memory address anywhere


class _DoubledYob(Attr):
    """A user subclass of ``Attr`` whose ``__call__`` computes something else."""

    def __call__(self, item: Any) -> Any:
        """Twice the attribute value (not expressible as a plain ``Attr``)."""
        return 2 * super().__call__(item)


def test_user_subclass_of_attr_extracts_as_opaque():
    """A user subclass of ``Attr`` as ranking key is extracted as ``Opaque``.
    Would fail if extraction used ``isinstance`` and wrote the subclass as a plain
    ``Attr`` record, so that a backend would rank by yob instead of 2 * yob."""
    root: PlanChild = (
        rank_by(_users(), ranking_key=_DoubledYob("yob")).to_plan().root
    )  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    key: Any = root.params["ranking_key"]  # the extracted ranking key
    assert isinstance(key, Opaque)  # a marker, not a structured expression
    assert _find_opaques(root) == [key]  # and the only marker in the plan


# -- Structured partition keys: Composite (008) -------------------------------

#: The IR record of ``Composite(Attr("department"), Lt("yob", 1980))``.
_COMPOSITE_RECORD: dict = {
    "type": "expression",
    "op": "composite",
    "components": [
        {"type": "expression", "op": "attr", "attr": "department"},
        {"type": "predicate", "op": "lt", "attr": "yob", "value": 1980},
    ],
}


def _partition_by_composite() -> partition:
    """A partition of the users by department and "born before 1980" (not executed)."""
    return partition(
        _users(), partitioning_function=Composite(Attr("department"), Lt("yob", 1980))
    )


def test_partition_with_composite_key_extracts_without_opaque():
    """``partition`` with a ``Composite`` of an ``Attr`` and a ``Lt`` is fully structured.
    Would fail if ``Composite`` were not recognized as serializable (e.g. because
    of its predicate component) or wrote a different IR record."""
    root: PlanChild = _partition_by_composite().to_plan().root  # no execution
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan
    key: Any = root.params["partitioning_function"]  # the extracted key
    assert key.to_dict() == _COMPOSITE_RECORD  # exact nested IR record


def test_partition_with_composite_key_survives_json_roundtrip():
    """A JSON round trip restores exactly the ``Composite`` partition key.
    Would fail if ``_value_from_dict`` did not restore the ``"composite"`` op,
    or restored its components as plain dicts instead of Attr / Lt objects."""
    restored: LogicalPlan = LogicalPlan.from_json(
        _partition_by_composite().to_plan().to_json()
    )  # serialize to JSON text and read it back
    root: PlanChild = restored.root  # the partition node
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    key: Any = root.params["partitioning_function"]  # the restored key
    assert type(key) is Composite  # exactly Composite, no dict
    assert [type(c) for c in key.components] == [
        Attr,
        Lt,
    ]  # components restored as objects, not as plain dicts
    assert key.to_dict() == _COMPOSITE_RECORD  # same components, same order


# -- Structured transform_items: Record (009) ---------------------------------

#: The IR record of the ``Record`` in ``_transform_by_record``; fields not sorted.
_RECORD_IR: dict = {
    "type": "expression",
    "op": "record",
    "fields": [
        {
            "name": "name",
            "value": {"type": "expression", "op": "attr", "attr": "name"},
        },
        {
            "name": "age",
            "value": {
                "type": "expression",
                "op": "sub",
                "left": 2026,
                "right": {"type": "expression", "op": "attr", "attr": "yob"},
            },
        },
        {
            "name": "early",
            "value": {"type": "predicate", "op": "lt", "attr": "yob", "value": 1980},
        },
    ],
}


def _transform_by_record() -> transform_items:
    """Users mapped to name, age and "born before 1980" (not executed)."""
    return transform_items(
        _users(),
        transformation_function=Record(
            name=Attr("name"), age=2026 - Attr("yob"), early=Lt("yob", 1980)
        ),
    )


def test_transform_items_with_record_extracts_without_opaque():
    """A ``Record`` in ``transform_items`` extracts structured as ``mapping_function``.
    Would fail if the ``Record`` became an ``Opaque`` marker, or if ``to_dict`` sorted
    the fields by name (``age`` before ``name``)."""
    root: PlanChild = _transform_by_record().to_plan().root  # no execution
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    assert _find_opaques(root) == []  # nothing opaque left in the whole plan
    record: Any = root.params["mapping_function"]  # the extracted Record
    assert record.to_dict() == _RECORD_IR  # exact IR record, fields in order


def test_transform_items_with_record_survives_json_roundtrip():
    """A JSON round trip restores exactly the ``Record``, with its field order.
    Would fail if the fields were stored as a JSON object (sorted keys on output)
    or restored as plain dicts instead of Attr / Sub / Lt objects."""
    pipeline: transform_items = _transform_by_record()  # not executed
    restored: LogicalPlan = LogicalPlan.from_json(
        pipeline.to_plan().to_json()
    )  # serialize to JSON text and read it back
    root: PlanChild = restored.root  # the transform_items node
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    record: Any = root.params["mapping_function"]  # the restored Record
    assert type(record) is Record  # exactly Record, no dict
    assert repr(record) == repr(
        pipeline.mapping_function
    )  # same fields, same order, same expression objects
    assert record.to_dict() == _RECORD_IR  # and the same IR record


class _MyMul(Mul):
    """A user subclass of ``Mul`` (could override ``__call__``)."""


@pytest.mark.parametrize(
    "record",
    [
        Record(x=_MyMul(Attr("yob"), 2)),
        Record(x=_DoubledYob("yob")),
        Record(x=_DoubledYob("yob") * 2),  # exact Mul, user Attr subclass as operand
    ],
    ids=["mul-subclass", "attr-subclass", "attr-subclass-as-mul-operand"],
)
def test_record_with_user_subclass_extracts_as_opaque(record: Record) -> None:
    """A ``Record`` holding a user subclass of ``Mul`` or ``Attr`` stays ``Opaque``.
    Would fail if the exact-type rule ignored record fields or arithmetic operands,
    so the plan would hold a plain record that computes something different."""
    assert record.is_serializable() is False  # not writable into the plan IR
    root: PlanChild = (
        transform_items(_users(), transformation_function=record).to_plan().root
    )  # extract without executing
    assert isinstance(root, PlanNode)  # an operator node, not a leaf
    marker: Any = root.params["mapping_function"]  # the extracted function
    assert isinstance(marker, Opaque)  # a marker, not a structured record
    assert _find_opaques(root) == [marker]  # and the only marker in the plan
