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

"""Structured, serializable predicates for FQL filter operators.

These predicate classes are callable (drop-in replacements for lambdas) and
serializable (the plan IR represents them as structured dicts instead of
``Opaque`` markers). This enables backend dispatchers to inspect and
translate filter conditions without executing Python code.

Usage with ``filter_values``::

    from fql.predicates import Eq, And, Like
    from fql.operators.filters import filter_values

    # Instead of: filter_values(users, filter_predicate=lambda v: v.name == "Alice")
    filter_values(users, filter_predicate=Eq("name", "Alice"))

    # Nested attribute paths (RF traversal):
    filter_values(users, filter_predicate=Eq("department.name", "Dev"))
    filter_values(users, filter_predicate=Eq("department__name", "Dev"))  # same

    # Composition:
    filter_values(users, filter_predicate=And(
        Eq("department.name", "Dev"),
        Gt("yob", 1980),
    ))

Attribute paths support both dot notation (``"department.name"``) and
double-underscore notation (``"department__name"``), consistent with
the existing ``.where()`` syntax on ``DictionaryAttributeFunction``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

# ---------------------------------------------------------------------------
# Attribute path resolution
# ---------------------------------------------------------------------------


class _Undefined:
    """Type of the single ``_UNDEFINED`` marker.

    A class rather than a bare ``object()`` so that the marker prints readably
    and stays the very same object when copied or pickled; consumers test it
    with ``is``.
    """

    def __repr__(self) -> str:
        """Print as ``UNDEFINED`` instead of ``<object at 0x...>``."""
        return "UNDEFINED"

    def __copy__(self) -> "_Undefined":
        """Keep identity: a copy is the marker itself."""
        return self

    def __deepcopy__(self, memo: dict) -> "_Undefined":
        """Keep identity: a deep copy is the marker itself."""
        return self

    def __reduce__(self) -> str:
        """Pickle by reference to the module-level name (same object on load)."""
        return "_UNDEFINED"


# Sentinel returned by ``_resolve_attr_path`` when a path segment is not mapped
# by the attribute function. Distinct from a stored ``None`` value: absence of a
# (key, value) mapping is not a value (FDM has no NULL). Predicates translate this
# into "does not match" so the tuple is simply not selected. Its public name is
# ``fql.expressions.UNDEFINED``.
_UNDEFINED: _Undefined = _Undefined()


def _split_attr_path(path: str) -> list[str]:
    """Split a dot- or dunder-separated attribute path into its segments.

    Double underscores are replaced by dots first. A run of three underscores
    therefore splits after the first two (``"a___b"`` becomes ``a`` -> ``_b``),
    as in ``.where()``; for names that start or end with an underscore, prefer
    the dot form. (Rejecting such ambiguous runs is a TODO for its own MR.)

    @param path: Dot- or ``__``-separated attribute path.
    @return: The path segments, in order.
    @raises ValueError: If the path contains an empty segment.
    """
    normalized: str = path.replace("__", ".")
    segments: list[str] = normalized.split(".")
    if not all(segments):
        raise ValueError(
            f"Empty segment in attribute path {path!r} (normalized: {normalized!r})"
        )
    return segments


def _resolve_attr_path(obj: Any, path: str) -> Any:
    """Traverse a dot-separated or dunder-separated attribute path on a value.

    ``"department.name"`` and ``"department__name"`` both resolve to the value
    stored under ``department`` then ``name``.

    The traversal is exception-free: at each segment it first asks the attribute
    function whether the key is mapped (``key in af`` — a plain dict membership
    test, no ``try/except``) and only then reads it. A missing mapping (or a
    non-AF intermediate value) yields the ``_UNDEFINED`` sentinel instead of
    raising ``AttributeError``. A stored ``None`` is a real value and is returned
    as-is.

    @param obj: The object to resolve the path on (typically a TF value).
    @param path: Dot-separated or ``__``-separated attribute path.
    @return: The resolved value, or ``_UNDEFINED`` if the path is not mapped.
    """
    # Local import avoids a circular import: fdm.attribute_functions imports this
    # module (for ``Predicate``), so this module cannot import it at top level.
    from fdm.attribute_functions import DictionaryAttributeFunction

    current: Any = obj
    for segment in _split_attr_path(path):
        # Only attribute functions can be traversed further; anything else (a
        # scalar leaf, or a missing key) means the path is not mapped here.
        if (
            not isinstance(current, DictionaryAttributeFunction)
            or segment not in current
        ):
            return _UNDEFINED
        current = current[segment]  # membership passed -> no exception possible
    return current


# ---------------------------------------------------------------------------
# Like matching
# ---------------------------------------------------------------------------


def _like_match(value: str, pattern: str) -> bool:
    """Simple SQL-style LIKE matching with ``%`` wildcards.

    Supported forms: ``"prefix%"``, ``"%suffix"``, ``"%contains%"``,
    ``"exact"`` (no wildcards), ``"%"`` (matches everything).

    Patterns with internal ``%`` wildcards (e.g. ``"H%st"``, ``"%a%b%"``)
    are **not** supported and will not match as expected.

    @param value: The string value to test.
    @param pattern: The pattern with optional ``%`` wildcards.
    @return: Whether the value matches the pattern.
    """
    if pattern == "%":
        return True
    if pattern.startswith("%") and pattern.endswith("%"):
        return pattern[1:-1] in value
    if pattern.startswith("%"):
        return value.endswith(pattern[1:])
    if pattern.endswith("%"):
        return value.startswith(pattern[:-1])
    return value == pattern


# ---------------------------------------------------------------------------
# Ref sentinel
# ---------------------------------------------------------------------------


class Ref:
    """Sentinel marking an attribute reference for attribute-to-attribute
    comparisons.

    When used as the ``value`` argument of a comparison predicate, the right-
    hand side is resolved against the same object as the left-hand side
    instead of being treated as a literal.

    Example::

        Gt("end_year", Ref("start_year"))  # end_year > start_year
    """

    def __init__(self, attr: str) -> None:
        """Initialize an attribute reference.

        @param attr: Dot- or ``__``-separated attribute path.
        """
        self.attr: str = attr

    def resolve(self, obj: Any) -> Any:
        """Resolve this reference against a value object.

        @param obj: The object to resolve the attribute path on.
        @return: The resolved attribute value.
        """
        return _resolve_attr_path(obj, self.attr)

    def to_dict(self) -> dict:
        """Serialize to a plain dict for the plan IR."""
        return {"type": "ref", "attr": self.attr}

    @classmethod
    def from_dict(cls, data: dict) -> Ref:
        """Reconstruct a ``Ref`` from a dict produced by ``to_dict``."""
        return cls(attr=data["attr"])

    def __repr__(self) -> str:
        return f"Ref({self.attr!r})"


# ---------------------------------------------------------------------------
# Predicate base class
# ---------------------------------------------------------------------------


class Predicate(ABC):
    """Abstract base class for structured, serializable filter predicates.

    Structured predicates are callable (drop-in replacements for lambdas)
    and serializable (so the plan IR can represent them as structured dicts
    instead of ``Opaque`` markers).

    Predicates operate on the value passed to them — typically a TF value
    (when used with ``filter_values``) or a key (when used with
    ``filter_keys``).
    """

    #: Registry mapping ``op`` strings to concrete predicate classes.
    #: Populated after all classes are defined at module level.
    _REGISTRY: dict[str, type[Predicate]] = {}

    @abstractmethod
    def __call__(self, value: Any) -> bool:
        """Evaluate this predicate on the given value.

        @param value: The value to test (typically a TF value).
        @return: ``True`` if the predicate is satisfied.
        """
        ...

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize this predicate to a plain dict for the plan IR.

        The dict always contains ``"type": "predicate"`` and an ``"op"``
        key identifying the concrete predicate class.
        """
        ...

    @classmethod
    def from_dict(cls, data: dict) -> Predicate:
        """Deserialize a predicate from a dict produced by ``to_dict``.

        Dispatches to the appropriate subclass based on the ``"op"`` key.

        @param data: A dict with ``"type": "predicate"`` and an ``"op"`` key.
        @return: A reconstructed ``Predicate`` instance.
        @raises ValueError: If the ``"op"`` value is not recognized.
        """
        op: str = data["op"]
        predicate_cls: type[Predicate] | None = cls._REGISTRY.get(op)
        if predicate_cls is None:
            raise ValueError(f"Unknown predicate op: {op!r}")
        return predicate_cls._from_dict(data)

    @classmethod
    @abstractmethod
    def _from_dict(cls, data: dict) -> Predicate:
        """Subclass-specific deserialization. Called by ``from_dict``."""
        ...


# ---------------------------------------------------------------------------
# Comparison predicates
# ---------------------------------------------------------------------------


class ComparisonPredicate(Predicate):
    """Base for binary comparison predicates (Eq, Gt, Lt, Gte, Lte).

    Compares a resolved attribute value against either a literal or another
    attribute (via ``Ref``).

    @param attr: Dot- or ``__``-separated attribute path on the input value.
    @param value: Literal value to compare against, or a ``Ref`` for
        attribute-to-attribute comparisons.
    """

    #: Subclasses set this to their operator name (e.g. ``"eq"``).
    op_name: str

    def __init__(self, attr: str, value: Any) -> None:
        """Initialize a comparison predicate.

        @param attr: Attribute path to resolve on the input value.
        @param value: Literal or ``Ref`` to compare against.
        """
        self.attr: str = attr
        self.value: Any = value

    def __call__(self, obj: Any) -> bool:
        """Evaluate the comparison on the given object.

        An unmapped attribute path means the tuple has no value to compare, so
        the predicate does not match (the tuple is not selected). This is not
        NULL semantics: there is simply no (key, value) mapping to test.
        """
        lhs: Any = _resolve_attr_path(obj, self.attr)
        rhs: Any = (
            self.value.resolve(obj) if isinstance(self.value, Ref) else self.value
        )
        # Either side unmapped -> nothing to compare -> not selected (two-valued:
        # no NULL, no UNKNOWN; a negation over this simply sees a plain False).
        if lhs is _UNDEFINED or rhs is _UNDEFINED:
            return False
        return self._compare(lhs, rhs)

    @abstractmethod
    def _compare(self, lhs: Any, rhs: Any) -> bool:
        """Perform the actual comparison. Implemented by subclasses."""
        ...

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": self.op_name,
            "attr": self.attr,
            "value": (
                self.value.to_dict() if isinstance(self.value, Ref) else self.value
            ),
        }

    @classmethod
    def _from_dict(cls, data: dict) -> ComparisonPredicate:
        """Reconstruct from a dict produced by ``to_dict``."""
        raw_value: Any = data["value"]
        value: Any = (
            Ref.from_dict(raw_value)
            if isinstance(raw_value, dict) and raw_value.get("type") == "ref"
            else raw_value
        )
        return cls(attr=data["attr"], value=value)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.attr!r}, {self.value!r})"


class Eq(ComparisonPredicate):
    """Equality predicate: ``attr == value``."""

    op_name: str = "eq"

    def _compare(self, lhs: Any, rhs: Any) -> bool:
        return lhs == rhs


class Gt(ComparisonPredicate):
    """Greater-than predicate: ``attr > value``."""

    op_name: str = "gt"

    def _compare(self, lhs: Any, rhs: Any) -> bool:
        return lhs > rhs


class Lt(ComparisonPredicate):
    """Less-than predicate: ``attr < value``."""

    op_name: str = "lt"

    def _compare(self, lhs: Any, rhs: Any) -> bool:
        return lhs < rhs


class Gte(ComparisonPredicate):
    """Greater-than-or-equal predicate: ``attr >= value``."""

    op_name: str = "gte"

    def _compare(self, lhs: Any, rhs: Any) -> bool:
        return lhs >= rhs


class Lte(ComparisonPredicate):
    """Less-than-or-equal predicate: ``attr <= value``."""

    op_name: str = "lte"

    def _compare(self, lhs: Any, rhs: Any) -> bool:
        return lhs <= rhs


# ---------------------------------------------------------------------------
# Like predicate
# ---------------------------------------------------------------------------


class Like(Predicate):
    """SQL-style LIKE predicate with ``%`` wildcards.

    Example::

        Like("name", "H%")    # names starting with "H"
        Like("name", "%ost")   # names ending with "ost"
        Like("name", "%or%")   # names containing "or"
    """

    def __init__(self, attr: str, pattern: str) -> None:
        """Initialize a LIKE predicate.

        @param attr: Attribute path to resolve on the input value.
        @param pattern: Pattern string with optional ``%`` wildcards.
        """
        self.attr: str = attr
        self.pattern: str = pattern

    def __call__(self, obj: Any) -> bool:
        """Evaluate the LIKE match on the given object."""
        val: Any = _resolve_attr_path(obj, self.attr)
        if val is _UNDEFINED:  # no mapping -> nothing to match -> not selected
            return False
        return _like_match(str(val), self.pattern)

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": "like",
            "attr": self.attr,
            "pattern": self.pattern,
        }

    @classmethod
    def _from_dict(cls, data: dict) -> Like:
        """Reconstruct from a dict produced by ``to_dict``."""
        return cls(attr=data["attr"], pattern=data["pattern"])

    def __repr__(self) -> str:
        return f"Like({self.attr!r}, {self.pattern!r})"


# ---------------------------------------------------------------------------
# In predicate
# ---------------------------------------------------------------------------


class In(Predicate):
    """Membership test: attribute value is in a given collection.

    Example::

        In("yob", [1972, 1983])

    .. note::
       ``to_dict()`` coerces ``values`` to a ``list``. After a JSON
       round-trip via ``from_dict``, ``values`` is always a ``list``
       regardless of the original container type.
    """

    def __init__(self, attr: str, values: list | tuple | set) -> None:
        """Initialize an IN predicate.

        @param attr: Attribute path to resolve on the input value.
        @param values: Collection of values to test membership against.
        """
        self.attr: str = attr
        self.values: list | tuple | set = values

    def __call__(self, obj: Any) -> bool:
        """Evaluate the membership test on the given object."""
        val: Any = _resolve_attr_path(obj, self.attr)
        if val is _UNDEFINED:  # no mapping -> nothing to test -> not selected
            return False
        return val in self.values

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": "in",
            "attr": self.attr,
            "values": list(self.values),
        }

    @classmethod
    def _from_dict(cls, data: dict) -> In:
        """Reconstruct from a dict produced by ``to_dict``."""
        return cls(attr=data["attr"], values=data["values"])

    def __repr__(self) -> str:
        return f"In({self.attr!r}, {self.values!r})"


# ---------------------------------------------------------------------------
# Existence predicates
# ---------------------------------------------------------------------------


class Exists(Predicate):
    """True iff the attribute path is mapped by the tuple.

    In FDM absence of a (key, value) mapping is structural — not a NULL value —
    so definedness is a first-class, directly queryable condition. Together with
    the two-valued comparison predicates this recovers everything SQL needs
    three-valued logic for, without introducing UNKNOWN::

        # "name is present and not Hugo" (namelesss tuples excluded):
        And(Exists("name"), Not(Eq("name", "Hugo")))

    A stored ``None`` counts as present (it is a real value).

    Example::

        Exists("person_role.name")  # tuple references a role that has a name
    """

    def __init__(self, attr: str) -> None:
        """Initialize an existence predicate.

        @param attr: Dot- or ``__``-separated attribute path to test.
        """
        self.attr: str = attr

    def __call__(self, obj: Any) -> bool:
        """True iff the path resolves to a mapping (does not raise on absence)."""
        return _resolve_attr_path(obj, self.attr) is not _UNDEFINED

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {"type": "predicate", "op": "exists", "attr": self.attr}

    @classmethod
    def _from_dict(cls, data: dict) -> Exists:
        """Reconstruct from a dict produced by ``to_dict``."""
        return cls(attr=data["attr"])

    def __repr__(self) -> str:
        return f"Exists({self.attr!r})"


class Missing(Predicate):
    """True iff the attribute path is not mapped by the tuple.

    The complement of :class:`Exists` (``Missing(a)`` == ``Not(Exists(a))``),
    provided as its own predicate so "the attribute is absent" reads directly.
    """

    def __init__(self, attr: str) -> None:
        """Initialize a missing-attribute predicate.

        @param attr: Dot- or ``__``-separated attribute path to test.
        """
        self.attr: str = attr

    def __call__(self, obj: Any) -> bool:
        """True iff the path does not resolve to a mapping."""
        return _resolve_attr_path(obj, self.attr) is _UNDEFINED

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {"type": "predicate", "op": "missing", "attr": self.attr}

    @classmethod
    def _from_dict(cls, data: dict) -> Missing:
        """Reconstruct from a dict produced by ``to_dict``."""
        return cls(attr=data["attr"])

    def __repr__(self) -> str:
        return f"Missing({self.attr!r})"


# ---------------------------------------------------------------------------
# Logical combinators
# ---------------------------------------------------------------------------


class And(Predicate):
    """Logical conjunction of two or more predicates.

    Example::

        And(Eq("department.name", "Dev"), Gt("yob", 1980))
    """

    def __init__(self, *predicates: Predicate) -> None:
        """Initialize a conjunction.

        @param predicates: Two or more predicates to combine.
        """
        self.predicates: tuple[Predicate, ...] = predicates

    def __call__(self, obj: Any) -> bool:
        """Evaluate: all child predicates must be satisfied."""
        return all(p(obj) for p in self.predicates)

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": "and",
            "predicates": [p.to_dict() for p in self.predicates],
        }

    @classmethod
    def _from_dict(cls, data: dict) -> And:
        """Reconstruct from a dict produced by ``to_dict``."""
        children: list[Predicate] = [Predicate.from_dict(p) for p in data["predicates"]]
        return cls(*children)

    def __repr__(self) -> str:
        inner: str = ", ".join(repr(p) for p in self.predicates)
        return f"And({inner})"


class Or(Predicate):
    """Logical disjunction of two or more predicates.

    Example::

        Or(Eq("name", "Alice"), Eq("name", "Bob"))
    """

    def __init__(self, *predicates: Predicate) -> None:
        """Initialize a disjunction.

        @param predicates: Two or more predicates to combine.
        """
        self.predicates: tuple[Predicate, ...] = predicates

    def __call__(self, obj: Any) -> bool:
        """Evaluate: at least one child predicate must be satisfied."""
        return any(p(obj) for p in self.predicates)

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": "or",
            "predicates": [p.to_dict() for p in self.predicates],
        }

    @classmethod
    def _from_dict(cls, data: dict) -> Or:
        """Reconstruct from a dict produced by ``to_dict``."""
        children: list[Predicate] = [Predicate.from_dict(p) for p in data["predicates"]]
        return cls(*children)

    def __repr__(self) -> str:
        inner: str = ", ".join(repr(p) for p in self.predicates)
        return f"Or({inner})"


class Not(Predicate):
    """Logical negation of a predicate.

    Example::

        Not(Eq("name", "Alice"))
    """

    def __init__(self, predicate: Predicate) -> None:
        """Initialize a negation.

        @param predicate: The predicate to negate.
        """
        self.predicate: Predicate = predicate

    def __call__(self, obj: Any) -> bool:
        """Evaluate: the child predicate must NOT be satisfied."""
        return not self.predicate(obj)

    def to_dict(self) -> dict:
        """Serialize to a plan-IR dict."""
        return {
            "type": "predicate",
            "op": "not",
            "predicate": self.predicate.to_dict(),
        }

    @classmethod
    def _from_dict(cls, data: dict) -> Not:
        """Reconstruct from a dict produced by ``to_dict``."""
        return cls(predicate=Predicate.from_dict(data["predicate"]))

    def __repr__(self) -> str:
        return f"Not({self.predicate!r})"


# ---------------------------------------------------------------------------
# Registry population
# ---------------------------------------------------------------------------

Predicate._REGISTRY = {
    "eq": Eq,
    "gt": Gt,
    "lt": Lt,
    "gte": Gte,
    "lte": Lte,
    "like": Like,
    "in": In,
    "exists": Exists,
    "missing": Missing,
    "and": And,
    "or": Or,
    "not": Not,
}
