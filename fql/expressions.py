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

Scope: ``Attr``, ``Key``, ``Composite`` and arithmetic (``Add``, ``Sub``,
``Mul``, ``Div``, ``Mod``, also written ``+ - * / %``) are meant as ranking keys
of ``rank_by``, ``subset`` (top-k) and ``items_sorted_by`` and as partition keys
of ``partition`` (and therefore ``group_by``); all of them drop items whose key
is ``UNDEFINED``. ``Record`` is meant only as the function of
``transform_items``. A ``Predicate`` may be used as a key too: it is evaluated on the
item's value and yields a bool, never ``UNDEFINED`` (two-valued logic of 004:
a comparison, ``Like``, ``In`` or ``Exists`` over a missing attribute gives
``False``; ``Missing(...)`` or the negation of a comparison gives ``True``;
``And`` / ``Or`` combine their parts; exactly as in ``filter_values``). Other
operators do not define a rule for the marker yet.
"""

from __future__ import annotations

import math
import operator
from abc import ABC, abstractmethod
from collections.abc import Mapping
from types import NotImplementedType
from typing import Any, Callable

from fdm.attribute_functions import TF
from fql.predicates.predicates import (
    _UNDEFINED,
    Predicate,
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

    # Arithmetic operators build arithmetic expressions (``Attr("a") * 2`` is
    # ``Mul(Attr("a"), 2)``; reflected variants keep the operand order). An
    # unsupported operand returns ``NotImplemented``, so Python raises
    # ``TypeError``. Comparisons are not overloaded: predicates exist, and
    # Python needs ``==`` for equality.
    def __add__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``self + other`` builds ``Add(self, other)``."""
        return _build_arithmetic(Add, self, other)

    def __radd__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``other + self`` builds ``Add(other, self)`` (operand order kept)."""
        return _build_arithmetic(Add, other, self)

    def __sub__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``self - other`` builds ``Sub(self, other)``."""
        return _build_arithmetic(Sub, self, other)

    def __rsub__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``other - self`` builds ``Sub(other, self)`` (operand order kept)."""
        return _build_arithmetic(Sub, other, self)

    def __mul__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``self * other`` builds ``Mul(self, other)``."""
        return _build_arithmetic(Mul, self, other)

    def __rmul__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``other * self`` builds ``Mul(other, self)`` (operand order kept)."""
        return _build_arithmetic(Mul, other, self)

    def __truediv__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``self / other`` builds ``Div(self, other)``."""
        return _build_arithmetic(Div, self, other)

    def __rtruediv__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``other / self`` builds ``Div(other, self)`` (operand order kept)."""
        return _build_arithmetic(Div, other, self)

    def __mod__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``self % other`` builds ``Mod(self, other)``."""
        return _build_arithmetic(Mod, self, other)

    def __rmod__(self, other: Any) -> _Arithmetic | NotImplementedType:
        """``other % self`` builds ``Mod(other, self)`` (operand order kept)."""
        return _build_arithmetic(Mod, other, self)

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize this expression to a plain dict for the plan IR."""
        ...

    def is_serializable(self) -> bool:
        """Whether this expression can be written into the plan IR.

        True only for the exact built-in classes (see ``_EXPRESSIONS_BY_OP``):
        a user subclass may override ``__call__`` and therefore stays
        ``Opaque``.
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


def _evaluate_on_item(key: Callable[[Item], Any] | Predicate, item: Item) -> Any:
    """Evaluate a ranking or partition key on ``item``.

    A ``Predicate`` is evaluated on ``item.value`` (as in ``filter_values``);
    anything else (an expression or a lambda) on the item itself. Shared by
    ``Composite``, ``partition`` and the sorting helper of ``rank_by`` /
    ``subset`` / ``items_sorted_by``.

    @param key: The key function, expression or predicate.
    @param item: The item to evaluate it on.
    @return: The key value (``UNDEFINED`` if an expression's attribute is unmapped).
    """
    if isinstance(key, Predicate):
        return key(item.value)
    return key(item)


class Composite(Expression):
    """A tuple of component values, e.g. ``Composite(Attr("dept"), Attr("city"))``.

    Returns ``UNDEFINED`` if any component is ``UNDEFINED``, so a tuple missing
    one of the attributes is dropped by the consuming operator. Components are
    expressions (``Attr``, ``Key``, arithmetic, ``Composite``) or predicates,
    not ``Record`` (a tuple of tuples has no use as a key); a predicate is
    evaluated on the item's value and yields a bool, never ``UNDEFINED``. Used
    for grouping by several attributes and as a lexicographic ranking key.
    """

    op_name: str = "composite"

    def __init__(self, *components: Expression | Predicate) -> None:
        """Initialize a composite expression.

        @param components: One or more expressions or predicates.
        @raises ValueError: If no component is given.
        @raises TypeError: If a component is neither an expression nor a
            predicate (e.g. a lambda or an attribute name as a plain string), or
            is a ``Record``.
        """
        if not components:
            raise ValueError("Composite needs at least one component")
        for component in components:
            if not isinstance(component, (Expression, Predicate)) or isinstance(
                component, Record
            ):
                raise TypeError(
                    f"Composite components must be expressions or predicates, "
                    f"got {component!r}"
                )
        self.components: tuple[Expression | Predicate, ...] = components

    def __call__(self, item: Item) -> Any:
        """Return the tuple of component values, or ``UNDEFINED``."""
        values: list[Any] = []
        for component in self.components:
            value: Any = _evaluate_on_item(component, item)
            if value is UNDEFINED:
                return UNDEFINED
            values.append(value)
        return tuple(values)

    def is_serializable(self) -> bool:
        """Whether this composite can be written into the plan IR.

        Requires exactly ``Composite`` and every expression component to be
        serializable. Predicate components always count as serializable,
        consistent with how the extractor treats a bare ``Predicate``.
        """
        return super().is_serializable() and all(
            isinstance(c, Predicate) or c.is_serializable() for c in self.components
        )

    def to_dict(self) -> dict:
        """Serialize to ``{"type": "expression", "op": "composite",
        "components": [...]}``."""
        return {
            "type": "expression",
            "op": self.op_name,
            "components": [c.to_dict() for c in self.components],
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> Composite:
        """Restore from a record; nested expression and predicate records are
        dispatched on their ``"type"``.

        @raises ValueError: If ``"components"`` is not a non-empty list, or a
            component is not a dict with a known ``"type"``, or a nested record
            has missing fields or the wrong shape (``KeyError`` / ``TypeError``
            from ``Predicate.from_dict`` are re-raised as ``ValueError``). Field
            values of the wrong type (e.g. a numeric ``"attr"``) are not checked
            here and only fail when the key is evaluated.
        """
        records: Any = data.get("components")
        if not isinstance(records, list) or not records:
            raise ValueError(
                f"Composite needs a non-empty list of components: {data!r}"
            )
        components: list[Expression | Predicate] = []
        for record in records:
            record_type: Any = (
                record.get("type") if isinstance(record, Mapping) else None
            )
            if record_type not in ("expression", "predicate"):
                raise ValueError(f"Malformed composite component: {record!r}")
            try:
                components.append(
                    Expression.from_dict(record)
                    if record_type == "expression"
                    else Predicate.from_dict(record)
                )
            except (KeyError, TypeError) as error:
                raise ValueError(
                    f"Malformed composite component: {record!r}"
                ) from error
        try:
            return cls(*components)
        except TypeError as error:  # e.g. a nested record component
            raise ValueError(f"Malformed composite component: {data!r}") from error

    def __repr__(self) -> str:
        """Readable form for ``explain()``, e.g. ``Composite(Attr('a'), Key())``."""
        return f"Composite({', '.join(repr(c) for c in self.components)})"


#: A valid arithmetic operand: an attribute, a key, nested arithmetic or a number.
type Operand = Attr | Key | _Arithmetic | int | float

#: A valid ``Record`` field value: a flat expression or a predicate (a bool).
type FieldValue = Attr | Key | _Arithmetic | Predicate


def _is_number(value: Any) -> bool:
    """Whether ``value`` is a numeric literal allowed in arithmetic.

    ``int`` (any size) or a finite ``float``; ``bool`` is excluded although it is
    an ``int`` subclass (``True + 1`` has no SQL counterpart). Only floats are
    checked for finiteness: ``math.isfinite`` would overflow on huge integers,
    and NaN / infinity cannot be written to standard JSON.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def _is_operand(value: Any) -> bool:
    """Whether ``value`` may be an arithmetic operand (see ``Operand``)."""
    return isinstance(value, (Attr, Key, _Arithmetic)) or _is_number(value)


def _build_arithmetic(
    arithmetic_cls: type[_Arithmetic], left: Any, right: Any
) -> _Arithmetic | NotImplementedType:
    """Build ``arithmetic_cls(left, right)`` for an operator overload.

    Returns ``NotImplemented`` for an unsupported operand, so that Python tries
    the other operand's reflected method and finally raises ``TypeError``.
    """
    if not (_is_operand(left) and _is_operand(right)):
        return NotImplemented
    return arithmetic_cls(left, right)


class _Arithmetic(Expression):
    """Binary arithmetic on numbers; shared base of ``Add``, ``Sub``, ``Mul``,
    ``Div`` and ``Mod``, which only set ``op_name`` and ``python_operator``.

    Evaluation, in this order: an ``UNDEFINED`` operand gives ``UNDEFINED``
    (absence never raises, 004); an operand value that is not an ``int`` /
    ``float`` (e.g. a stored ``None`` or a string) raises ``TypeError``; for
    ``Div`` / ``Mod`` a zero divisor gives ``UNDEFINED`` (the field is absent);
    otherwise Python semantics apply -- integers do not overflow, but mixing a
    very large integer with a float (or ``/`` on it) may raise
    ``OverflowError``.
    """

    #: The Python operator applied to the two operand values.
    python_operator: Callable[[Any, Any], Any]
    #: Whether a zero right operand yields ``UNDEFINED`` (``Div``, ``Mod``).
    zero_divisor_is_undefined: bool = False

    def __init__(self, left: Operand, right: Operand) -> None:
        """Initialize a binary arithmetic expression.

        @param left: Left operand (see ``Operand``).
        @param right: Right operand (see ``Operand``).
        @raises TypeError: If an operand is not an ``Attr``, ``Key``, arithmetic
            expression or numeric literal (e.g. a string, ``bool``, ``None``,
            NaN, a ``Composite``, a ``Record`` or a predicate).
        """
        for operand in (left, right):
            if not _is_operand(operand):
                raise TypeError(
                    f"{type(self).__name__} operands must be Attr, Key, arithmetic "
                    f"expressions or finite int/float numbers, got {operand!r}"
                )
        self.left: Operand = left
        self.right: Operand = right

    def __call__(self, item: Item) -> Any:
        """Evaluate both operands on ``item`` and combine them (see class doc)."""
        left_value, right_value = (
            operand(item) if isinstance(operand, Expression) else operand
            for operand in (self.left, self.right)
        )
        if left_value is UNDEFINED or right_value is UNDEFINED:
            return UNDEFINED
        for value in (left_value, right_value):
            # Values from the data: bool is excluded as in literals; NaN or
            # infinity stored in the data are numbers and pass through.
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"{self!r}: operand value {value!r} is not a number")
        if self.zero_divisor_is_undefined and right_value == 0:
            return UNDEFINED  # division by zero: the field is absent (004)
        return self.python_operator(left_value, right_value)

    def is_serializable(self) -> bool:
        """Exact built-in class and every expression operand serializable."""
        return super().is_serializable() and all(
            not isinstance(operand, Expression) or operand.is_serializable()
            for operand in (self.left, self.right)
        )

    def to_dict(self) -> dict:
        """Serialize to ``{"type": "expression", "op": ..., "left": ...,
        "right": ...}``; numeric operands are written as plain JSON numbers."""

        def serialized(operand: Operand) -> Any:
            """An expression operand as its record, a number as itself."""
            return operand.to_dict() if isinstance(operand, Expression) else operand

        return {
            "type": "expression",
            "op": self.op_name,
            "left": serialized(self.left),
            "right": serialized(self.right),
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> _Arithmetic:
        """Restore from a record; an operand is a nested expression record or a
        JSON number.

        @raises ValueError: If ``"left"`` / ``"right"`` is missing or not a valid
            operand.
        """

        def restored(operand: Any) -> Any:
            """A nested record as its expression, a number as itself."""
            if isinstance(operand, Mapping):
                if operand.get("type") != "expression":
                    raise ValueError(f"Malformed arithmetic operand: {operand!r}")
                return Expression.from_dict(operand)
            return operand

        if "left" not in data or "right" not in data:
            raise ValueError(f"Arithmetic needs left and right: {data!r}")
        try:
            return cls(restored(data["left"]), restored(data["right"]))
        except TypeError as error:
            raise ValueError(f"Malformed arithmetic operand: {data!r}") from error

    def __repr__(self) -> str:
        """Readable form for ``explain()``, e.g. ``Mul(Attr('score'), 2)``."""
        return f"{type(self).__name__}({self.left!r}, {self.right!r})"


class Add(_Arithmetic):
    """``left + right``. SQL: ``left + right``."""

    op_name: str = "add"
    python_operator = staticmethod(operator.add)


class Sub(_Arithmetic):
    """``left - right``. SQL: ``left - right``."""

    op_name: str = "sub"
    python_operator = staticmethod(operator.sub)


class Mul(_Arithmetic):
    """``left * right``. SQL: ``left * right``."""

    op_name: str = "mul"
    python_operator = staticmethod(operator.mul)


class Div(_Arithmetic):
    """True division ``left / right`` (``7 / 2 == 3.5``); a zero divisor gives
    ``UNDEFINED``. SQL: ``CAST(left AS DOUBLE PRECISION) / NULLIF(right, 0)``."""

    op_name: str = "div"
    python_operator = staticmethod(operator.truediv)
    zero_divisor_is_undefined = True


class Mod(_Arithmetic):
    """Python modulo ``left % right``: the result takes the sign of the divisor
    (``-7 % 3 == 2``); a zero divisor gives ``UNDEFINED``. SQL:
    ``((left % NULLIF(right, 0)) + right) % right``."""

    op_name: str = "mod"
    python_operator = staticmethod(operator.mod)
    zero_divisor_is_undefined = True


def _check_field_name(name: Any) -> None:
    """Reject field names ``Attr`` could not read back.

    ``Attr`` (and ``TF.__getitem__``) split attribute paths at ``.`` and ``__``,
    so a field named ``"a.b"`` or ``"a__b"`` would be unreachable.

    @raises ValueError: If ``name`` is not a non-empty string without ``.``
        or ``__``.
    """
    if not isinstance(name, str) or not name or "." in name or "__" in name:
        raise ValueError(f"Invalid Record field name: {name!r}")


class Record(Expression):
    """A new tuple with exactly the given fields, in the given order, e.g.
    ``Record(name=Attr("name"), score=Attr("score") * 2)`` (SQL:
    ``SELECT name, score * 2 AS score``).

    Fields are flat values (see ``FieldValue``); a predicate field yields a
    bool. A field whose value is ``UNDEFINED`` is absent from the result; if
    every field is absent the result is an empty tuple. Meant only as the
    function of ``transform_items``; as a ranking or partition key it makes no
    sense (documented, not enforced).
    """

    op_name: str = "record"

    # The positional-only ``self`` lets a field be named ``self``.
    def __init__(self, /, **fields: FieldValue) -> None:
        """Initialize a record expression.

        @param fields: Field name -> ``Attr``, ``Key``, arithmetic or predicate.
        @raises ValueError: If no field is given or a name is invalid (see
            ``_check_field_name``).
        @raises TypeError: If a value is a constant, a ``Composite``, a nested
            ``Record`` or anything else.
        """
        if not fields:
            raise ValueError("Record needs at least one field")
        for name, value in fields.items():
            _check_field_name(name)
            if not isinstance(value, (Attr, Key, _Arithmetic, Predicate)):
                raise TypeError(
                    f"Record field {name!r} must be Attr, Key, an arithmetic "
                    f"expression or a predicate, got {value!r}"
                )
        self.fields: dict[str, FieldValue] = fields

    def __call__(self, item: Item) -> TF:
        """Return a new, unfrozen TF with every field whose value is defined."""
        result: TF = TF(frozen=False)
        for name, value_expression in self.fields.items():
            value: Any = _evaluate_on_item(value_expression, item)
            if value is not UNDEFINED:
                result[name] = value
        return result

    def is_serializable(self) -> bool:
        """Exact ``Record`` and every expression field serializable; predicate
        fields count as serializable (as for ``Composite``)."""
        return super().is_serializable() and all(
            isinstance(value, Predicate) or value.is_serializable()
            for value in self.fields.values()
        )

    def to_dict(self) -> dict:
        """Serialize to ``{"type": "expression", "op": "record", "fields":
        [{"name": ..., "value": ...}, ...]}`` -- an ordered list, because the
        JSON output sorts dict keys and would lose the field order."""
        return {
            "type": "expression",
            "op": self.op_name,
            "fields": [
                {"name": name, "value": value.to_dict()}
                for name, value in self.fields.items()
            ],
        }

    @classmethod
    def _from_dict(cls, data: Mapping[str, Any]) -> Record:
        """Restore from a record, keeping the field order.

        @raises ValueError: If ``"fields"`` is not a non-empty list, an entry is
            malformed, a name is invalid or repeated (a list can repeat names,
            keyword arguments cannot), or a value is not a valid field value.
        """
        entries: Any = data.get("fields")
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"Record needs a non-empty list of fields: {data!r}")
        fields: dict[str, FieldValue] = {}
        for entry in entries:
            if not isinstance(entry, Mapping) or not isinstance(
                entry.get("value"), Mapping
            ):
                raise ValueError(f"Malformed Record field: {entry!r}")
            name: Any = entry.get("name")
            _check_field_name(name)
            if name in fields:
                raise ValueError(f"Duplicate Record field: {name!r}")
            value: Mapping[str, Any] = entry["value"]
            try:
                fields[name] = (
                    Predicate.from_dict(value)
                    if value.get("type") == "predicate"
                    else Expression.from_dict(value)
                )
            except (KeyError, TypeError) as error:
                raise ValueError(f"Malformed Record field: {entry!r}") from error
        try:
            return cls(**fields)
        except TypeError as error:
            raise ValueError(f"Malformed Record field: {data!r}") from error

    def __repr__(self) -> str:
        """Readable form for ``explain()``, e.g. ``Record(name=Attr('name'))``."""
        return f"Record({', '.join(f'{k}={v!r}' for k, v in self.fields.items())})"


#: Maps an op name to the expression class that restores it.
_EXPRESSIONS_BY_OP: dict[str, type[Expression]] = {
    expression_cls.op_name: expression_cls
    for expression_cls in (Attr, Key, Composite, Add, Sub, Mul, Div, Mod, Record)
}
