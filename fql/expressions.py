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
"""Structured, serializable expressions for FQL.

An ``Expression`` is a drop-in replacement for a lambda that is evaluated on an
``Item`` (e.g. a ranking key), but, unlike a lambda, it can be written into the
plan IR as data. Lambdas stay allowed; they appear as ``Opaque`` in plans.

Return contract: an expression may return ``UNDEFINED`` when the accessed
attribute is not mapped. FDM has no NULL: absence is not a value, and a tuple
without the accessed attribute does not contribute to the operator reading it
(see ``docs/changes/004-undefined-attribute-semantics/DECISION.md``). Every
consumer of an expression must handle this marker.

Scope: for now ``Attr`` and ``Key`` are meant as ranking keys of ``rank_by``,
``subset`` (top-k) and ``items_sorted_by``, which drop items whose key is
``UNDEFINED``. Other operators (e.g. ``partition``) do not yet define a rule for
the marker; there an unmapped attribute would silently form its own group.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from fql.predicates.predicates import (
    _UNDEFINED,
    _Undefined,
    _resolve_attr_path,
    _split_attr_path,
)
from fql.util import Item

#: Returned by an expression whose attribute is not mapped for an item. It is
#: the same object predicates and aggregates use internally.
UNDEFINED: _Undefined = _UNDEFINED


class Expression(ABC):
    """Base class for structured expressions evaluated on an ``Item``.

    Mirrors ``Predicate``: callable (a drop-in for a lambda), serializable via
    ``to_dict`` / ``from_dict``, and readable via ``__repr__``.
    """

    #: Operator name in the plan IR (e.g. ``"attr"``).
    op_name: str

    @abstractmethod
    def __call__(self, item: Item) -> Any:
        """Evaluate this expression on ``item``; may return ``UNDEFINED``."""
        ...

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize this expression to a plain dict for the plan IR."""
        ...

    def is_serializable(self) -> bool:
        """Whether this expression can be written into the plan IR.

        True only for the exact built-in classes (``Attr``, ``Key``): a user
        subclass may override ``__call__`` and therefore stays ``Opaque``.
        """
        return type(self) in _EXPRESSIONS_BY_OP.values()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Expression:
        """Deserialize an expression from a dict produced by ``to_dict``.

        Always dispatches on ``"op"``, also when called on a subclass.

        @param data: A dict with ``"type": "expression"`` and an ``"op"`` key.
        @return: A new instance of the built-in class registered for ``"op"``.
        @raises ValueError: If ``"type"`` is not ``"expression"``, ``"op"`` is
            missing or unknown, or the record is malformed for that op.
        """
        if data.get("type") != "expression":
            raise ValueError(f"Not an expression record: {data!r}")
        op: Any = data.get("op")
        expression_cls: type[Expression] | None = (
            _EXPRESSIONS_BY_OP.get(op) if isinstance(op, str) else None
        )
        if expression_cls is None:
            raise ValueError(f"Unknown expression op: {op!r}")
        return expression_cls._from_dict(data)

    @classmethod
    @abstractmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> Expression:
        """Subclass-specific deserialization. Called by ``from_dict``."""
        ...


class Attr(Expression):
    """The value of an attribute path of ``item.value``, e.g. ``Attr("yob")``.

    Paths may use dots or double underscores (``"department.name"`` or
    ``"department__name"``). Returns ``UNDEFINED`` if the path is not mapped.
    Only attribute functions are traversed: on scalar values, or with a
    misspelt attribute, every item yields ``UNDEFINED``.

    Not to be confused with ``Ref``: a ``Ref`` is the right-hand operand of a
    comparison predicate and is resolved on a value; an ``Attr`` is a
    stand-alone expression evaluated on an ``Item``.
    """

    op_name: str = "attr"

    def __init__(self, attr: str) -> None:
        """Initialize an attribute expression.

        @param attr: Dot- or ``__``-separated attribute path.
        @raises ValueError: If ``attr`` is not a non-empty path without empty
            segments (checked with the same splitting as at evaluation time).
        """
        if not isinstance(attr, str):
            raise ValueError(f"Attribute path must be a string: {attr!r}")
        _split_attr_path(attr)  # raises ValueError on empty segments
        self.attr: str = attr

    def __call__(self, item: Item) -> Any:
        """Return the attribute value of ``item.value``, or ``UNDEFINED``."""
        return _resolve_attr_path(item.value, self.attr)

    def to_dict(self) -> dict:
        """Serialize to ``{"type": "expression", "op": "attr", "attr": ...}``."""
        return {"type": "expression", "op": self.op_name, "attr": self.attr}

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> Attr:
        """Restore from a record; the constructor validates ``"attr"``."""
        return cls(data.get("attr"))

    def __repr__(self) -> str:
        """Readable form for ``explain()``, e.g. ``Attr('yob')``."""
        return f"Attr({self.attr!r})"


class Key(Expression):
    """The key of the item, ``item.key``."""

    op_name: str = "key"

    def __call__(self, item: Item) -> Any:
        """Return ``item.key``."""
        return item.key

    def to_dict(self) -> dict:
        """Serialize to ``{"type": "expression", "op": "key"}``."""
        return {"type": "expression", "op": self.op_name}

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> Key:
        """Restore from a record; ``Key`` has no parameters."""
        return cls()

    def __repr__(self) -> str:
        """Readable form for ``explain()``: ``Key()``."""
        return "Key()"


#: Maps an op name to the expression class that restores it.
_EXPRESSIONS_BY_OP: dict[str, type[Expression]] = {
    expression_cls.op_name: expression_cls for expression_cls in (Attr, Key)
}
