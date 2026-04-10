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

"""Logical IR for extracted FQL operator pipelines.

This module defines the small set of immutable dataclasses that make up the
logical plan produced by ``fql.plan.extract``. The IR is deliberately minimal
and backend-agnostic:

* ``LeafRef`` identifies a concrete ``AttributeFunction`` at the leaves of the
  pipeline (by its integer UUID, optionally enriched with a schema binding
  name and the concrete AF class name for debuggability).
* ``PlanNode`` represents an unary FQL operator invocation with its named
  parameters and its (currently always: single) input subplan. In FDM all
  operators are unary; operators that logically combine multiple inputs do so
  via a single ``DBF`` input (see the JOB example). A ``DBF`` binding of named
  sub-AFs is surfaced in the IR as a synthetic ``op="DBF_bind"`` node whose
  ``params["names"]`` carries the binding names and whose ``inputs`` carry the
  bound sub-plans in the same order.
* ``Opaque`` marks any value that could not be serialized (primarily: Python
  lambdas / arbitrary callables used as predicates). It records *why* the
  value is opaque, a best-effort textual representation, and the CPython
  ``id()`` of the original object so that a local executor can look the
  original Python callable back up by identity without the IR ever needing to
  actually carry it.
* ``LogicalPlan`` is a thin wrapper around a root node that adds JSON
  serialization, a human-readable pretty-printer, and an IR-version tag for
  forward-compatibility.

Nothing in this module depends on any runtime Operator or AttributeFunction
state; it is pure data. The only place that knows how to *build* an IR from
an operator tree is ``fql.plan.extract``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Union

# -- Version ------------------------------------------------------------------

#: Monotonically increasing integer tag stamped into every serialized plan.
#: Bump this when the on-wire IR format changes in a way that older readers
#: cannot handle, so that future consumers can refuse or migrate old plans
#: instead of silently misinterpreting them.
IR_VERSION: int = 1


# -- Nodes --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LeafRef:
    """Reference to a concrete ``AttributeFunction`` at a pipeline leaf.

    The IR does not embed the AF itself — only enough identity to look it up
    on the executing side. ``uuid`` is the canonical identifier (see
    ``fdm.API.AttributeFunction.uuid``); ``af_class`` and ``schema_name`` are
    carried for debuggability and for human-readable explains, and should not
    be relied on as lookup keys.
    """

    #: Discriminator. Currently always ``"af"`` — reserved for future leaf
    #: kinds (e.g. external tables, constants) so the IR format does not need
    #: a version bump just to add them.
    kind: Literal["af"]

    #: Unqualified class name of the concrete AF (e.g. ``"TF"``, ``"RF"``,
    #: ``"DBF"``). Informational only.
    af_class: str

    #: Integer UUID from ``AttributeFunction.uuid``. May be ``None`` only for
    #: leaves that are not yet instantiated / registered; real extractions
    #: will always set this.
    uuid: int | None = None

    #: Optional human-friendly binding name, typically the key under which
    #: the AF appears in an enclosing ``DBF`` (e.g. ``"chn"`` in the JOB
    #: example). Purely for readability.
    schema_name: str | None = None

    def to_dict(self) -> dict:
        return {
            "type": "leaf",
            "kind": self.kind,
            "af_class": self.af_class,
            "uuid": self.uuid,
            "schema_name": self.schema_name,
        }


@dataclass(frozen=True, slots=True)
class Opaque:
    """Placeholder for a value that could not be serialized to the IR.

    The primary use is the ``filter_predicate`` / ``join_predicate`` style
    Python lambdas that FQL operators accept as black-boxes. Instead of
    failing the extraction, we record a marker so that:

    * a backend dispatcher can detect opaque subtrees and partition the plan
      (run the opaque part locally, ship the rest), and
    * a local executor can resolve ``py_id`` back to the original Python
      object via an out-of-band lookup table if it needs to execute the
      opaque node itself.

    .. warning::
       Serialized ``Opaque`` markers carry two pieces of information that
       are potentially sensitive:

       * ``repr`` contains the raw ``repr()`` of the original Python object.
         If that object is a closure over credentials, tokens, PII, or a
         configured database connection, those will appear verbatim in the
         IR. Do not ship extracted plans to untrusted consumers without
         scrubbing or truncating this field.
       * ``py_id`` is a CPython memory address (from ``id()``). It is only
         meaningful in-process and leaks a small amount of information
         about the process's memory layout. Strip it before persisting
         plans to disk or transporting them off-box.
    """

    #: High-level reason this value was not serializable. One of
    #: ``"lambda"``, ``"callable"``, ``"unknown"``. Free-form for now.
    reason: str

    #: Best-effort textual representation of the opaque value. For lambdas
    #: this is typically ``repr(value)``; callers may upgrade this to a
    #: source snippet via ``inspect.getsource`` in a later revision.
    repr: str

    #: CPython ``id()`` of the original object at extraction time. Stable for
    #: the lifetime of the object in the same process only; do not persist
    #: across processes.
    py_id: int

    def to_dict(self) -> dict:
        return {
            "type": "opaque",
            "reason": self.reason,
            "repr": self.repr,
            "py_id": self.py_id,
        }


#: Anything that can appear as a child under a ``PlanNode.inputs`` entry.
PlanChild = Union["PlanNode", LeafRef]


@dataclass(frozen=True, slots=True)
class PlanNode:
    """A single logical operator invocation in the extracted pipeline.

    Operator identity is carried as a bare class-name string so the IR does
    not depend on the actual Python ``Operator`` subclasses being importable
    on the consuming side (important for cross-process / cross-language
    backends).
    """

    #: Class name of the originating ``Operator`` subclass, e.g.
    #: ``"filter_values"``, ``"equi_join"``, ``"DBF_bind"``.
    op: str

    #: Tuple of child subplans. Always a tuple (not list) so ``PlanNode`` can
    #: remain hashable / frozen. In FDM all operators are unary, so real
    #: operator nodes have exactly one child; the synthetic ``"DBF_bind"``
    #: node is the one place where multiple children appear.
    inputs: tuple[PlanChild, ...] = ()

    #: JSON-ish keyword parameters. Values are one of: primitive
    #: (str/int/float/bool/None), tuple/list of those, a nested dict of
    #: those, or an ``Opaque`` marker. The extractor is responsible for
    #: ensuring this invariant holds; see ``fql.plan.extract._serialize_param``.
    params: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "type": "node",
            "op": self.op,
            "inputs": [_child_to_dict(c) for c in self.inputs],
            "params": {k: _value_to_dict(v) for k, v in self.params.items()},
        }


# -- Serialization helpers ----------------------------------------------------


def _child_to_dict(child: PlanChild) -> dict:
    """Serialize a ``PlanNode``/``LeafRef`` child to a plain dict."""
    if isinstance(child, (PlanNode, LeafRef)):
        return child.to_dict()
    # Defensive: should not happen if the extractor is well-behaved, but we
    # do not want to crash serialization on a stray literal. Wrap it as an
    # opaque-ish record so the consumer can see what went wrong.
    return {"type": "literal", "value": _value_to_dict(child)}


def _value_to_dict(value: Any) -> Any:
    """Serialize a parameter value to a JSON-friendly structure.

    This is intentionally narrow: it only accepts the shapes that the
    extractor is allowed to produce. Anything else is coerced into an
    ``Opaque`` dict so the IR stays serializable by construction.
    """
    if isinstance(value, Opaque):
        return value.to_dict()
    if isinstance(value, (LeafRef, PlanNode)):
        return value.to_dict()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_value_to_dict(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _value_to_dict(v) for k, v in value.items()}
    # Fallback: coerce to Opaque so the JSON dump never fails silently.
    return Opaque(
        reason="unknown",
        repr=repr(value),
        py_id=id(value),
    ).to_dict()


# -- LogicalPlan wrapper ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogicalPlan:
    """A root-and-metadata wrapper around an extracted plan tree.

    The wrapper exists so that the on-wire format can carry an ``ir_version``
    and any future metadata (e.g. extraction timestamp, schema hash) without
    polluting ``PlanNode``. It is the only type intended to cross a process
    boundary.
    """

    root: PlanChild
    ir_version: int = IR_VERSION

    def to_dict(self) -> dict:
        return {
            "ir_version": self.ir_version,
            "root": _child_to_dict(self.root),
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        """Serialize to a JSON string. ``indent=None`` produces compact output."""
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LogicalPlan":
        """Rehydrate a ``LogicalPlan`` from a dict produced by ``to_dict``.

        ``Opaque`` nodes come back as ``Opaque`` instances but their
        ``py_id`` is *not* useful across processes — the consumer must treat
        them as unresolved and either reject the plan or partition it.
        """
        version = data.get("ir_version", IR_VERSION)
        if version != IR_VERSION:
            # v1: accept only our own version. A later revision can add real
            # migrations here.
            raise ValueError(
                f"Unsupported IR version: {version} (expected {IR_VERSION})"
            )
        return cls(root=_child_from_dict(data["root"]), ir_version=version)

    @classmethod
    def from_json(cls, s: str) -> "LogicalPlan":
        return cls.from_dict(json.loads(s))

    def explain(self) -> str:
        """Produce a human-readable, indented pretty-print of the plan."""
        lines: list[str] = []
        _explain_into(self.root, 0, lines)
        return "\n".join(lines)


# -- Deserialization ----------------------------------------------------------


def _child_from_dict(data: Mapping[str, Any]) -> PlanChild:
    """Inverse of ``_child_to_dict`` for ``PlanNode``/``LeafRef`` children."""
    t = data.get("type")
    if t == "leaf":
        return LeafRef(
            kind=data["kind"],
            af_class=data["af_class"],
            uuid=data.get("uuid"),
            schema_name=data.get("schema_name"),
        )
    if t == "node":
        return PlanNode(
            op=data["op"],
            inputs=tuple(_child_from_dict(c) for c in data.get("inputs", ())),
            params={k: _value_from_dict(v) for k, v in data.get("params", {}).items()},
        )
    raise ValueError(f"Unknown plan child type: {t!r}")


def _value_from_dict(value: Any) -> Any:
    """Inverse of ``_value_to_dict``."""
    if isinstance(value, dict):
        t = value.get("type")
        if t == "opaque":
            return Opaque(
                reason=value["reason"],
                repr=value["repr"],
                py_id=value["py_id"],
            )
        if t in ("leaf", "node"):
            return _child_from_dict(value)
        if t == "literal":
            return _value_from_dict(value["value"])
        # plain dict: recurse
        return {k: _value_from_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_value_from_dict(v) for v in value]
    return value


# -- Pretty-print -------------------------------------------------------------


def _explain_into(node: PlanChild, depth: int, out: list[str]) -> None:
    """Append a pretty-printed view of ``node`` into ``out``."""
    pad = "  " * depth
    if isinstance(node, LeafRef):
        name = node.schema_name or f"#{node.uuid}"
        out.append(f"{pad}- leaf {node.af_class} {name}")
        return
    if isinstance(node, PlanNode):
        if node.params:
            params_repr = ", ".join(
                f"{k}={_short_param(v)}" for k, v in node.params.items()
            )
            out.append(f"{pad}- {node.op}({params_repr})")
        else:
            out.append(f"{pad}- {node.op}")
        for child in node.inputs:
            _explain_into(child, depth + 1, out)
        return
    out.append(f"{pad}- <literal {node!r}>")


def _short_param(value: Any) -> str:
    """Short, single-line rendering of a parameter value for ``explain()``."""
    if isinstance(value, Opaque):
        return f"<opaque {value.reason}>"
    if isinstance(value, (LeafRef, PlanNode)):
        return f"<{value.__class__.__name__}>"
    return repr(value)
