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

"""Extraction walker: FQL operator tree -> ``LogicalPlan``.

The key invariant of this module: **no operator is ever executed.** We walk
the tree by reading the ``input_function`` attribute directly and must never
call ``.result`` / ``__call__`` / ``_resolve_input`` on an ``Operator``.

In FDM, all operators are unary. Operators that look multi-input (joins, set
ops) take a single ``DBF`` that in turn binds several sub-AFs by name. We
surface that binding as a synthetic ``"DBF_bind"`` node in the IR so the
named structure survives extraction.

Lambdas and other arbitrary Python callables passed as predicates are not
introspected; they are replaced with an ``Opaque`` marker carrying a
best-effort textual representation and the object's ``id()``. A downstream
dispatcher can use the presence of ``Opaque`` to partition the plan into a
backend-executable prefix and a locally-executable residual.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fql.plan.ir import LeafRef, LogicalPlan, Opaque, PlanChild, PlanNode


def extract(node: Any) -> PlanChild:
    """Walk ``node`` and return its logical plan representation.

    ``node`` may be:

    * an ``fql.operators.APIs.Operator`` instance (the common case),
    * a bare ``AttributeFunction`` (returned as a ``LeafRef``),
    * a ``Mapping`` that looks like a DBF binding dict, or
    * anything else (returned wrapped as a literal / opaque as appropriate,
      though this branch is mainly here for robustness — real callers should
      pass an operator or an AF).

    Never triggers operator execution.
    """
    # Local imports keep ``fql.plan`` importable even when the Operator
    # / AttributeFunction modules are still being loaded.
    from fdm.API import AttributeFunction
    from fql.operators.APIs import Operator

    if isinstance(node, Operator):
        return _extract_operator(node)

    if isinstance(node, AttributeFunction):
        # DBFs are the FDM way of composing named sub-AFs (e.g. relations
        # in a database). We extract their children into a ``DBF_bind`` node
        # so the named structure survives in the plan — a backend needs those
        # names to resolve joins implied by foreign-object constraints.
        from fdm.attribute_functions import DBF

        if isinstance(node, DBF):
            return _bind_from_af_items(node)
        return _leaf_from_af(node)

    if isinstance(node, Mapping):
        # Bare dicts are also accepted for robustness, but the idiomatic
        # FDM way is to use a ``DBF``.
        return _bind_from_af_items(node)

    # Fallback: represent as an opaque literal wrapped in a synthetic node so
    # the caller still gets a PlanChild back (never a raw Python value).
    return PlanNode(
        op="literal",
        inputs=(),
        params={"value": _serialize_param(node)},
    )


def extract_plan(node: Any) -> LogicalPlan:
    """Convenience wrapper returning a ready-to-serialize ``LogicalPlan``."""
    return LogicalPlan(root=extract(node))


# -- Operator extraction ------------------------------------------------------


def _extract_operator(op: Any) -> PlanNode:
    """Extract a single ``Operator`` node (recurses into its input)."""
    # Subclasses may override these hooks; the base class provides defaults
    # that cover the common case (one ``input_function`` field, all other
    # public fields become params).
    inputs_raw = list(op._plan_inputs())
    params_raw: Mapping[str, Any] = op._plan_params()

    inputs: tuple[PlanChild, ...] = tuple(extract(i) for i in inputs_raw)
    params = {k: _serialize_param(v) for k, v in params_raw.items()}

    return PlanNode(op=type(op).__name__, inputs=inputs, params=params)


# -- Leaves -------------------------------------------------------------------


def _leaf_from_af(af: Any) -> LeafRef:
    """Build a ``LeafRef`` from an ``AttributeFunction`` instance."""
    # ``uuid`` is always assigned in ``AttributeFunction.__init__`` (see
    # ``fdm/API.py``), but we read defensively via getattr in case a subclass
    # ever bypasses the base constructor.
    uuid = getattr(af, "uuid", None)
    return LeafRef(
        kind="af",
        af_class=type(af).__name__,
        uuid=uuid if isinstance(uuid, int) else None,
        schema_name=None,
    )


def _bind_from_af_items(source: Any) -> PlanNode:
    """Turn a ``DBF`` or a bare ``{name: sub_plan_or_af}`` mapping into a
    ``DBF_bind`` node.

    The synthetic op name ``"DBF_bind"`` is reserved in the IR vocabulary for
    this purpose and must not collide with a real ``Operator`` subclass name.

    For a ``DBF``, iteration yields ``Item(key, value)`` pairs; for a plain
    ``Mapping``, we iterate ``.items()`` which yields ``(key, value)`` tuples.
    Both are normalized into the same ``DBF_bind`` structure.
    """
    from fdm.attribute_functions import DBF
    from fql.util import Item

    names: list[str] = []
    children: list[PlanChild] = []

    # DBF iterates as Item(key, value); plain dicts iterate as (key, value).
    if isinstance(source, DBF):
        pairs = ((str(item.key), item.value) for item in source)
    else:
        pairs = ((str(k), v) for k, v in source.items())

    for name, value in pairs:
        names.append(name)
        child = extract(value)
        # If the source is itself a leaf, stamp the binding name onto it so
        # ``explain()`` can show something like "leaf RF chn" instead of
        # "leaf RF #42". This is purely cosmetic — the IR does not otherwise
        # rely on ``schema_name``.
        if isinstance(child, LeafRef) and child.schema_name is None:
            child = LeafRef(
                kind=child.kind,
                af_class=child.af_class,
                uuid=child.uuid,
                schema_name=name,
            )
        children.append(child)
    return PlanNode(
        op="DBF_bind",
        inputs=tuple(children),
        params={"names": tuple(names)},
    )


# -- Parameter serialization --------------------------------------------------


def _serialize_param(value: Any) -> Any:
    """Coerce an arbitrary operator parameter into a JSON-friendly shape.

    Rules (in order):

    1. Primitives and ``None`` pass through unchanged.
    2. Tuples and lists are recursed element-wise (tuples become lists in
       the JSON form, matching ``ir._value_to_dict``).
    3. Dicts are recursed value-wise with string-coerced keys.
    4. Nested ``AttributeFunction``s and ``Operator``s — which can show up
       in parameters for operators that take multiple AF handles — are
       recursed through ``extract``.
    5. Anything callable (lambdas, functions, bound methods, classes with
       ``__call__``) becomes an ``Opaque`` marker. This is the whole point
       of the v1 extractor: we do not attempt to introspect Python code.
    6. Anything else falls back to ``Opaque("unknown", ...)`` rather than
       crashing the extraction.
    """
    from fdm.API import AttributeFunction
    from fql.operators.APIs import Operator

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (list, tuple)):
        return [_serialize_param(v) for v in value]

    if isinstance(value, Mapping):
        return {str(k): _serialize_param(v) for k, v in value.items()}

    if isinstance(value, (AttributeFunction, Operator)):
        return extract(value)

    # Structured predicates are callable but serializable — detect them
    # before the generic callable fallback so they appear as structured
    # dicts in the IR instead of Opaque markers.
    from fql.predicates.predicates import Predicate

    if isinstance(value, Predicate):
        return value

    if callable(value):
        # Lambdas show up as ``<function <lambda> at 0x...>`` under repr; we
        # keep that verbatim — it is at least as informative as anything we
        # could synthesize without AST introspection.
        reason = (
            "lambda" if getattr(value, "__name__", "") == "<lambda>" else "callable"
        )
        return Opaque(reason=reason, repr=repr(value), py_id=id(value))

    return Opaque(reason="unknown", repr=repr(value), py_id=id(value))
