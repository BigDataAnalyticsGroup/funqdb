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
"""Unit tests for the structured expressions ``Attr``, ``Key``, ``Composite``, the
arithmetic expressions (``Add``, ``Sub``, ``Mul``, ``Div``, ``Mod``) and ``Record``."""

import copy
import pickle
from typing import Any, Callable

import pytest

from fdm.attribute_functions import TF
from fql.expressions import (
    UNDEFINED,
    Add,
    Attr,
    Composite,
    Div,
    Expression,
    Key,
    Mod,
    Mul,
    Record,
    Sub,
)
from fql.predicates.predicates import _UNDEFINED, Gt, Lt
from fql.util import Item


def _employee() -> Item:
    """One employee item with a nested department and a stored ``None``.

    ::

        key 7 -> TF
                 +-- name       = "Ann"
                 +-- manager    = None        <- stored None, a real value
                 +-- yob        = 1970        <- scalar, cannot be traversed
                 +-- department -> TF
                                   +-- name = "R&D"
    """
    return Item(
        key=7,
        value=TF(
            {
                "name": "Ann",
                "manager": None,
                "yob": 1970,
                "department": TF({"name": "R&D"}),
            }
        ),
    )  # a single item as the operators pass it to a ranking key


class _DoubledYob(Attr):
    """A user subclass of ``Attr`` that changes what it computes."""

    def __call__(self, item: Item) -> Any:
        """Twice the attribute value (not expressible as a plain ``Attr``)."""
        return 2 * super().__call__(item)


# -- Attr: evaluation ----------------------------------------------------------


@pytest.mark.parametrize("path", ["department.name", "department__name"])
def test_attr_dot_and_dunder_paths_reach_the_nested_value(path: str) -> None:
    """``Attr`` resolves a nested path written with dots or with double underscores.
    Would fail if one of the two path spellings were not normalized the same way."""
    assert Attr(path)(_employee()) == "R&D"  # the department's name


@pytest.mark.parametrize(
    "path",
    ["salary", "office.name", "department.budget", "yob.decade"],
    ids=["missing-leaf", "missing-intermediate", "missing-in-nested", "scalar-hop"],
)
def test_attr_unmapped_path_yields_undefined(path: str) -> None:
    """An unmapped path, or one that runs through a scalar, yields ``UNDEFINED``.
    Would fail if a broken path raised ``AttributeError`` or returned ``None``."""
    assert Attr(path)(_employee()) is UNDEFINED  # absence is not a value


def test_attr_on_scalar_item_value_yields_undefined() -> None:
    """``Attr`` on an item whose value is a scalar (not a TF) yields ``UNDEFINED``.
    Would fail if ``Attr`` tried ``getattr`` on the scalar and raised instead."""
    item: Item = Item(key=1, value=5)  # e.g. an item of a plain number mapping
    assert Attr("yob")(item) is UNDEFINED  # an int has no attributes to resolve


def test_attr_returns_stored_none_as_a_value() -> None:
    """A stored ``None`` is returned as ``None``, not as ``UNDEFINED``.
    Would fail if ``Attr`` conflated a stored ``None`` with an unmapped attribute,
    which would make ranking drop tuples that do have the attribute."""
    result: Any = Attr("manager")(_employee())  # the attribute is mapped to None
    assert result is None  # the stored value itself


def test_attr_triple_underscore_splits_after_first_dunder() -> None:
    """``Attr("a___b")`` reads the path ``a`` -> ``_b``, same as ``Attr("a._b")``.
    Would fail if the documented split of a three-underscore run changed silently.

    This pins the documented *current* behaviour (see ``_split_attr_path`` in
    ``fql/predicates/predicates.py``); rejecting such ambiguous paths is a TODO
    for a separate MR, which will have to update this test.

    ::

        item.value -> TF
                      +-- a -> TF
                              +-- _b = "hit"     <- reached by "a___b"
    """
    item: Item = Item(
        key=1, value=TF({"a": TF({"_b": "hit"})})
    )  # a maps to a TF that holds _b
    assert Attr("a___b")(item) == "hit"  # "__" -> "." first: "a._b"
    assert Attr("a._b")(item) == "hit"  # the explicit dot form reads the same value


# -- Attr: construction --------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    ["", "a..b", "a.", "__x", "x__"],
    ids=["empty", "double-dot", "trailing-dot", "leading-dunder", "trailing-dunder"],
)
def test_attr_rejects_path_with_empty_segment(path: str) -> None:
    """``Attr`` rejects a path with an empty segment already when it is created.
    Would fail if the check were deferred to evaluation time, where an invalid
    path would surface only once the query runs."""
    with pytest.raises(ValueError, match="Empty segment in attribute path"):  # eager
        Attr(path)  # construction alone must fail


@pytest.mark.parametrize("path", [5, None], ids=["int", "none"])
def test_attr_rejects_non_string_path(path: Any) -> None:
    """``Attr`` rejects a path that is not a string with a ``ValueError``.
    Would fail if a non-string reached ``str.replace`` and raised ``AttributeError``."""
    with pytest.raises(ValueError, match="Attribute path must be a string"):  # clear
        Attr(path)  # construction alone must fail


# -- Key -----------------------------------------------------------------------


def test_key_returns_the_item_key() -> None:
    """``Key()`` returns the key of the item, not its value.
    Would fail if ``Key`` returned ``item.value`` or the whole item."""
    assert Key()(_employee()) == 7  # the key of the employee item


@pytest.mark.parametrize(
    "expression, expected",
    [(Attr("department__name"), "Attr('department__name')"), (Key(), "Key()")],
    ids=["attr", "key"],
)
def test_expression_repr_is_readable(expression: Expression, expected: str) -> None:
    """``repr`` of ``Attr`` and ``Key`` reads like the constructor call.
    Would fail if ``Attr`` normalized the path in its ``repr`` or if either fell
    back to the default ``<... object at 0x...>`` form."""
    assert repr(expression) == expected  # exactly as written by the user


# -- UNDEFINED marker ----------------------------------------------------------


def test_undefined_is_the_predicate_marker_and_prints_readably() -> None:
    """The public ``UNDEFINED`` is the predicates' marker and prints as ``UNDEFINED``.
    Would fail if expressions and predicates used two different markers, so that
    ``is UNDEFINED`` checks in consumers missed one of them."""
    assert UNDEFINED is _UNDEFINED  # one single marker object
    assert repr(UNDEFINED) == "UNDEFINED"  # readable, no memory address


@pytest.mark.parametrize(
    "duplicate",
    [copy.copy, copy.deepcopy, lambda value: pickle.loads(pickle.dumps(value))],
    ids=["copy", "deepcopy", "pickle"],
)
def test_undefined_identity_survives_copy_and_pickle(
    duplicate: Callable[[Any], Any],
) -> None:
    """Copying or pickling ``UNDEFINED`` gives back the very same object.
    Would fail if a copy produced a new instance: consumers test the marker
    with ``is``, so a copied marker would be treated as a real value."""
    assert duplicate(UNDEFINED) is UNDEFINED  # identity preserved


# -- Serialization -------------------------------------------------------------


@pytest.mark.parametrize(
    "record, expected_type, expected_repr",
    [
        ({"type": "expression", "op": "attr", "attr": "a__b"}, Attr, "Attr('a__b')"),
        ({"type": "expression", "op": "key"}, Key, "Key()"),
    ],
    ids=["attr", "key"],
)
def test_from_dict_restores_exact_expression(
    record: dict, expected_type: type, expected_repr: str
) -> None:
    """``Expression.from_dict`` restores exactly ``Attr`` or ``Key`` from a record.
    Would fail if the op registry mapped to the wrong class or ``Attr`` lost or
    rewrote its path."""
    restored: Expression = Expression.from_dict(record)  # read the IR record
    assert type(restored) is expected_type  # exact class, not a subclass
    assert repr(restored) == expected_repr  # same path as in the record
    assert restored.to_dict() == record  # round trip back to the same record


@pytest.mark.parametrize(
    "record, message",
    [
        ({"type": "predicate", "op": "key"}, "Not an expression record"),
        ({"op": "key"}, "Not an expression record"),
        ({"type": "expression", "op": "len"}, "Unknown expression op"),
        ({"type": "expression"}, "Unknown expression op"),
        ({"type": "expression", "op": 1}, "Unknown expression op"),
        ({"type": "expression", "op": "attr"}, "Attribute path must be a string"),
        (
            {"type": "expression", "op": "attr", "attr": 5},
            "Attribute path must be a string",
        ),
        (
            {"type": "expression", "op": "attr", "attr": ""},
            "Empty segment in attribute path",
        ),
    ],
    ids=[
        "wrong-type",
        "missing-type",
        "unknown-op",
        "missing-op",
        "non-string-op",
        "attr-missing",
        "attr-non-string",
        "attr-empty",
    ],
)
def test_from_dict_rejects_malformed_record(record: dict, message: str) -> None:
    """``Expression.from_dict`` raises ``ValueError`` for every malformed record.
    Would fail if a malformed plan raised ``KeyError`` / ``TypeError`` or, worse,
    silently restored an expression with a missing or wrong attribute path."""
    with pytest.raises(ValueError, match=message):  # one clear error type
        Expression.from_dict(record)  # read the malformed record


def test_from_dict_on_subclass_dispatches_on_op() -> None:
    """``Attr.from_dict`` of a ``"key"`` record returns a ``Key`` (documented).
    Would fail if ``from_dict`` used the class it was called on instead of the
    ``"op"`` registry, e.g. by building an ``Attr`` without a path."""
    restored: Expression = Attr.from_dict(
        {"type": "expression", "op": "key"}
    )  # called on Attr, record says key
    assert type(restored) is Key  # the op decides, not the receiver class


@pytest.mark.parametrize(
    "expression, expected",
    [(Attr("yob"), True), (Key(), True), (_DoubledYob("yob"), False)],
    ids=["attr", "key", "user-subclass"],
)
def test_is_serializable_only_for_exact_builtin_classes(
    expression: Expression, expected: bool
) -> None:
    """Only the exact built-in classes are serializable, not user subclasses.
    Would fail if ``is_serializable`` used ``isinstance``: a subclass overriding
    ``__call__`` would be written as a plain ``Attr`` and lose its logic."""
    assert expression.is_serializable() is expected  # exact-type rule


# -- Composite (008) -----------------------------------------------------------


def test_composite_rejects_zero_components() -> None:
    """``Composite()`` without any component is rejected when it is created.
    Would fail if an empty composite were accepted and every item got the key ``()``."""
    with pytest.raises(
        ValueError, match="needs at least one component"
    ):  # eager check in the constructor
        Composite()  # no components at all


@pytest.mark.parametrize(
    "component",
    ["a", lambda item: 1],
    ids=["bare-string", "lambda"],
)
def test_composite_rejects_non_expression_component(component: Any) -> None:
    """``Composite`` rejects a component that is neither an expression nor a predicate.
    Would fail if a bare attribute name or a lambda were accepted: the string would
    fail only at evaluation, the lambda would make the composite unserializable."""
    with pytest.raises(
        TypeError, match="must be expressions or predicates"
    ):  # eager check in the constructor
        Composite(component)  # construction alone must fail


@pytest.mark.parametrize(
    "record, message",
    [
        (
            {"type": "expression", "op": "composite"},
            "non-empty list of components",
        ),
        (
            {"type": "expression", "op": "composite", "components": []},
            "non-empty list of components",
        ),
        (
            {
                "type": "expression",
                "op": "composite",
                "components": [{"type": "expression", "attr": "a"}],
            },
            "Unknown expression op",  # Expression.from_dict's own ValueError
        ),
        (
            {
                "type": "expression",
                "op": "composite",
                "components": [{"type": "column", "op": "attr", "attr": "a"}],
            },
            "Malformed composite component",
        ),
        (
            {
                "type": "expression",
                "op": "composite",
                "components": [{"type": "predicate", "op": "gt"}],
            },
            "Malformed composite component",
        ),  # predicate record without attr / value: Predicate.from_dict's KeyError
        (
            {
                "type": "expression",
                "op": "composite",
                "components": [{"type": "predicate", "op": "not", "predicate": 5}],
            },
            "Malformed composite component",
        ),  # wrong shape below op: Predicate.from_dict's TypeError
    ],
    ids=[
        "missing-components",
        "empty-components",
        "component-without-op",
        "component-unknown-type",
        "predicate-component-without-fields",
        "predicate-component-wrong-shape",
    ],
)
def test_composite_from_dict_rejects_malformed_record(
    record: dict, message: str
) -> None:
    """``Expression.from_dict`` raises ``ValueError`` for a malformed composite record.
    Would fail if a malformed composite, or a malformed predicate record nested in
    it, raised ``KeyError`` / ``TypeError`` instead of one clear error type."""
    with pytest.raises(ValueError, match=message):  # one clear error type
        Expression.from_dict(record)  # read the malformed record


class _MyComposite(Composite):
    """A user subclass of ``Composite`` (could override ``__call__``)."""


@pytest.mark.parametrize(
    "composite",
    [_MyComposite(Attr("yob")), Composite(Attr("name"), _DoubledYob("yob"))],
    ids=["composite-subclass", "user-attr-component"],
)
def test_composite_with_user_subclass_is_not_serializable(
    composite: Composite,
) -> None:
    """A ``Composite`` subclass or a composite with a user ``Attr`` subclass is opaque.
    Would fail if the check used ``isinstance`` or ignored the components, so the
    plan would hold a plain record that computes something different."""
    assert composite.is_serializable() is False  # stays Opaque in the plan


def test_nested_composite_repr_is_readable() -> None:
    """``repr`` of a nested composite reads like the nested constructor calls.
    Would fail if an inner composite or predicate fell back to the default
    ``<... object at 0x...>`` form."""
    composite: Composite = Composite(
        Attr("dept"), Composite(Key(), Lt("yob", 1980))
    )  # a composite inside a composite
    assert (
        repr(composite) == "Composite(Attr('dept'), Composite(Key(), Lt('yob', 1980)))"
    )  # exactly as written by the user


def test_nested_composite_returns_nested_tuple_and_propagates_undefined() -> None:
    """A nested ``Composite`` yields a nested tuple, and ``UNDEFINED`` from the inner
    composite makes the outer one ``UNDEFINED`` as well.
    Would fail if the inner tuple were flattened into the outer one, or if the outer
    composite returned ``("Ann", UNDEFINED)`` (the tuple would not be dropped)."""
    item: Item = _employee()  # key 7, name Ann, yob 1970, no salary
    assert Composite(Attr("name"), Composite(Key(), Lt("yob", 1980)))(item) == (
        "Ann",
        (7, True),
    )  # inner composite stays a nested tuple
    assert (
        Composite(Attr("name"), Composite(Key(), Attr("salary")))(item) is UNDEFINED
    )  # no salary -> inner UNDEFINED -> outer UNDEFINED


def test_composite_with_predicate_component_keeps_tuple_missing_the_attribute() -> None:
    """A predicate component over a missing attribute yields ``False``, not ``UNDEFINED``.
    Would fail if ``Composite`` treated a predicate like an ``Attr`` and dropped the
    tuple (decision 008: ``Composite(Attr("a"), Gt("b", 1))`` gives ``(a, False)``)."""
    item: Item = _employee()  # key 7, name Ann, no salary mapped
    assert Composite(Attr("name"), Lt("salary", 1))(item) == (
        "Ann",
        False,
    )  # kept, with False for the comparison over the missing salary


# -- Arithmetic expressions (009) ----------------------------------------------


def _scores() -> Item:
    """One item with numbers for arithmetic; no ``salary`` is mapped.

    ::

        key 3 -> TF
                 +-- name   = "Ann"
                 +-- score  = 7
                 +-- bonus  = 2
                 +-- debt   = -7
    """
    return Item(
        key=3, value=TF({"name": "Ann", "score": 7, "bonus": 2, "debt": -7})
    )  # a single item as transform_items passes it to a Record


@pytest.mark.parametrize(
    "expression, expected",
    [
        (Attr("score") * 2, 14),  # operator overload builds Mul
        (Div(Attr("score"), Attr("bonus")), 3.5),  # true division, as 7 / 2
        (Mod(Attr("debt"), 3), 2),  # sign of the divisor, as -7 % 3
        (Attr("score") + Attr("bonus"), 9),  # 7 + 2, both operands attributes
        (Attr("score") / Attr("bonus"), 3.5),  # operator / builds Div: 7 / 2
        (Attr("debt") % 3, 2),  # operator % builds Mod: -7 % 3
        ((Attr("score") + 1) * 2, 16),  # nested: (7 + 1) * 2, not 7 + 1 * 2 = 9
        (Attr("score") - Attr("bonus"), 5),  # operator - builds Sub: 7 - 2, not 2 - 7
    ],
    ids=[
        "mul",
        "div",
        "mod",
        "add",
        "div-operator",
        "mod-operator",
        "nested",
        "sub",
    ],
)
def test_arithmetic_evaluates_with_python_semantics(
    expression: Expression, expected: float
) -> None:
    """Arithmetic over attributes gives the same value as Python on the stored numbers.
    Would fail if ``/`` did integer division, ``%`` took the sign of the dividend
    (as in SQL), or a nested expression lost its grouping."""
    assert expression(_scores()) == expected  # see the parametrized expectation


@pytest.mark.parametrize(
    "expression, expected_type, expected_repr, expected",
    [
        (10 - Attr("score"), Sub, "Sub(10, Attr('score'))", 3),  # 10 - 7, not -3
        (14 / Attr("score"), Div, "Div(14, Attr('score'))", 2.0),  # 14 / 7, not 0.5
        (10 % Attr("score"), Mod, "Mod(10, Attr('score'))", 3),  # 10 % 7, not 7
        (2 + Attr("score"), Add, "Add(2, Attr('score'))", 9),  # 2 + 7, Add(2, ...)
        (2 * Attr("score"), Mul, "Mul(2, Attr('score'))", 14),  # 2 * 7, Mul(2, ...)
    ],
    ids=[
        "rsub",
        "rtruediv",
        "rmod",
        "radd",
        "rmul",
    ],
)
def test_reflected_operator_keeps_literal_on_the_left(
    expression: Any, expected_type: type, expected_repr: str, expected: float
) -> None:
    """A literal on the left (``10 - Attr("score")``) stays the left operand.
    Would fail if ``__radd__`` / ``__rsub__`` / ``__rmul__`` / ``__rtruediv__`` /
    ``__rmod__`` passed their operands in the order of the non-reflected method
    (e.g. 7 - 10 = -3)."""
    assert type(expression) is expected_type  # an expression object, not a number
    assert repr(expression) == expected_repr  # literal is the left operand
    assert expression(_scores()) == expected  # literal OP 7, not 7 OP literal


@pytest.mark.parametrize(
    "expression",
    [
        Attr("score") + Key(),
        Div(Attr("score"), 2.5),
        Mod(Mul(Attr("score"), -1), Attr("bonus")),
    ],
    ids=["add", "div", "mod-nested"],
)
def test_arithmetic_survives_plan_record_roundtrip(expression: Expression) -> None:
    """``from_dict(to_dict())`` restores ``Add``, ``Div`` and ``Mod`` exactly.
    Would fail if an op name were missing from the registry, a float literal were
    rounded, or a nested operand were restored as a plain dict."""
    ir_record: dict = expression.to_dict()  # the plan IR record
    restored: Expression = Expression.from_dict(ir_record)  # read it back
    assert type(restored) is type(expression)  # exact class, not a subclass
    assert repr(restored) == repr(expression)  # same operands, same order
    assert restored.to_dict() == ir_record  # and the same IR record again


def test_arithmetic_with_missing_attribute_yields_undefined() -> None:
    """A missing attribute makes the arithmetic and all arithmetic around it undefined.
    Would fail if ``UNDEFINED`` reached the Python operator (``TypeError``) or the
    inner result were not propagated through the outer expression."""
    assert (Attr("salary") + 1)(_scores()) is UNDEFINED  # no salary mapped
    assert ((Attr("salary") + 1) * 2)(_scores()) is UNDEFINED  # propagates outward


# -- Record (009) --------------------------------------------------------------


def test_record_builds_tuple_with_exactly_the_given_fields_in_order() -> None:
    """``Record`` builds a new tuple with exactly its fields, in the given order.
    Would fail if fields were sorted by name, if attributes of the input tuple not
    listed in the record leaked into the result, or a predicate field were not a bool.

    ::

        input:  name="Ann", score=7, bonus=2, debt=-7      (key 3)
        output: score=14, id=3, name="Ann", low=False
    """
    record: Record = Record(
        score=Attr("score") * 2,  # arithmetic field
        id=Key(),  # the item key as a field
        name=Attr("name"),  # copied attribute
        low=Lt("score", 5),  # predicate field: 7 < 5 is False
    )  # field names deliberately not in alphabetical order
    result: TF = record(_scores())  # evaluate on the single item
    assert [(f.key, f.value) for f in result] == [
        ("score", 14),
        ("id", 3),
        ("name", "Ann"),
        ("low", False),
    ]  # exactly these fields, in the order of the Record; bonus and debt are gone
    assert result.frozen is False  # unfrozen, like a tuple built by a lambda


def test_record_leaves_out_a_field_that_evaluates_to_undefined() -> None:
    """A field over a missing attribute is absent from the result; the others stay.
    Would fail if the field were stored as ``UNDEFINED`` or ``None``, or if the whole
    tuple were dropped instead of only the field."""
    record: Record = Record(
        name=Attr("name"), pay=Attr("salary") * 2
    )  # no salary mapped on the item
    result: TF = record(_scores())  # evaluate on the single item
    assert [(f.key, f.value) for f in result] == [
        ("name", "Ann")
    ]  # only name; pay is absent, not None


# -- Arithmetic and Record: edge cases and failures (009) ----------------------

#: Python's own ``TypeError`` when an operator overload returns ``NotImplemented``
#: on both sides; ``str`` on the left reports its concatenation error instead.
_PYTHON_OPERATOR_ERROR: str = r"unsupported operand type|can only concatenate"


def _mixed() -> Item:
    """One item with a zero and with non-numeric values next to a number.

    ::

        key 1 -> TF
                 +-- n       = 7
                 +-- zero    = 0
                 +-- manager = None     <- stored None, not a number
                 +-- name    = "Ann"    <- a string
                 +-- active  = True     <- a bool (an int subclass in Python)
    """
    return Item(
        key=1,
        value=TF({"n": 7, "zero": 0, "manager": None, "name": "Ann", "active": True}),
    )  # a single item as the operators pass it to an expression


@pytest.mark.parametrize(
    "expression",
    [
        Div(Attr("n"), 0),
        Mod(Attr("n"), 0),
        Div(Attr("n"), 0.0),
        Attr("n") / Attr("zero"),
        Attr("n") % Attr("zero"),
    ],
    ids=["div-literal", "mod-literal", "div-float-zero", "div-attr", "mod-attr"],
)
def test_division_or_modulo_by_zero_yields_undefined(expression: Expression) -> None:
    """Division and modulo by zero (literal or stored) yield ``UNDEFINED``.
    Would fail if ``ZeroDivisionError`` escaped or the zero check missed ``0.0``."""
    assert expression(_mixed()) is UNDEFINED  # absent, as SQL a / NULLIF(b, 0)


@pytest.mark.parametrize("attr", ["manager", "name", "active"])
def test_arithmetic_on_non_numeric_value_raises_type_error(attr: str) -> None:
    """A stored ``None``, string or bool operand raises ``TypeError`` at evaluation.
    Would fail if Python's operator ran on it (``"Ann" * 2``, ``True * 2`` succeed)."""
    with pytest.raises(TypeError, match="is not a number"):  # the explicit check
        (Attr(attr) * 2)(_mixed())  # evaluate on the item


def test_arithmetic_undefined_wins_over_non_numeric_operand() -> None:
    """A missing attribute gives ``UNDEFINED`` even if the other operand is a string.
    Would fail if the number check ran before the absence check and raised."""
    expression: Expression = Attr("salary") + Attr("name")  # no salary; name is str
    assert expression(_mixed()) is UNDEFINED  # absence wins, no TypeError


@pytest.mark.parametrize(
    "build, message",
    [
        (lambda bad: Attr("a") + bad, _PYTHON_OPERATOR_ERROR),
        (lambda bad: bad + Attr("a"), _PYTHON_OPERATOR_ERROR),
        (lambda bad: Mul(Attr("a"), bad), "Mul operands must be Attr, Key"),
        (lambda bad: Mul(bad, Attr("a")), "Mul operands must be Attr, Key"),
    ],
    ids=["op-right", "op-left", "class-right", "class-left"],
)
@pytest.mark.parametrize(
    "bad",
    [
        "x",
        True,
        None,
        float("nan"),
        float("inf"),
        Composite(Attr("a")),
        Record(a=Attr("a")),
        Gt("a", 1),
        lambda item: 1,
    ],
    ids=[
        "str",
        "bool",
        "none",
        "nan",
        "inf",
        "composite",
        "record",
        "predicate",
        "lambda",
    ],
)
def test_arithmetic_rejects_invalid_operand(
    build: Callable[[Any], Any],
    message: str,
    bad: Any,
) -> None:
    """An invalid operand on either side is rejected with ``TypeError`` at construction.
    Would fail if e.g. a bool, NaN or a predicate were accepted as an operand."""
    with pytest.raises(TypeError, match=message):  # eager check, no expression built
        build(bad)  # operator overload or explicit class, either side


def test_arithmetic_accepts_very_large_int_literal() -> None:
    """``Attr("n") + 10**400`` builds and evaluates with exact integer arithmetic.
    Would fail if the finiteness check converted the int to float (``OverflowError``).
    """
    expression: Expression = Attr("n") + 10**400  # larger than any float
    assert expression(_mixed()) == 10**400 + 7  # Python ints do not overflow


def test_record_with_every_field_absent_yields_empty_tuple() -> None:
    """A ``Record`` whose fields are all absent yields an empty TF, not ``UNDEFINED``.
    Would fail if the record returned ``UNDEFINED`` or ``None`` for such an item."""
    result: Any = Record(pay=Attr("salary"), q=Attr("n") / 0)(_mixed())  # both absent
    assert type(result) is TF  # still a tuple
    assert len(result) == 0  # with no fields at all


def test_record_field_may_be_named_self() -> None:
    """A field named ``self`` is allowed (the constructor's ``self`` is positional).
    Would fail with "got multiple values for argument 'self'" otherwise."""
    result: TF = Record(self=Attr("name"))(_mixed())  # field literally named self
    assert [(f.key, f.value) for f in result] == [("self", "Ann")]  # one field


@pytest.mark.parametrize(
    "fields, message",
    [
        ({}, "at least one field"),
        ({"a.b": Attr("n")}, "Invalid Record field name"),
        ({"a__b": Attr("n")}, "Invalid Record field name"),
        ({"": Attr("n")}, "Invalid Record field name"),
    ],
    ids=["no-fields", "dot", "dunder", "empty-name"],
)
def test_record_rejects_invalid_field_names(fields: dict, message: str) -> None:
    """``Record`` rejects no fields and names ``Attr`` could not read back.
    Would fail if a field ``a.b`` were built that ``Attr("a.b")`` reads as a path."""
    with pytest.raises(ValueError, match=message):  # eager check
        Record(**fields)  # construction alone must fail


@pytest.mark.parametrize(
    "value",
    [1, "name", Composite(Attr("n")), Record(a=Attr("n"))],
    ids=["constant", "bare-string", "composite", "nested-record"],
)
def test_record_rejects_invalid_field_value(value: Any) -> None:
    """``Record`` rejects constants, bare strings and nested ``Composite`` / ``Record``.
    Would fail if ``"name"`` were stored as a constant instead of read as attribute."""
    with pytest.raises(TypeError, match="must be Attr, Key"):  # eager check
        Record(x=value)  # construction alone must fail


def test_composite_rejects_record_component() -> None:
    """``Composite`` rejects a ``Record`` component, also when read from a plan record.
    Would fail if a TF became a partition key or ``from_dict`` leaked a ``TypeError``.
    """
    with pytest.raises(TypeError, match="must be expressions or predicates"):  # eager
        Composite(Record(a=Attr("n")))  # constructor
    record: dict = {
        "type": "expression",
        "op": "composite",
        "components": [Record(a=Attr("n")).to_dict()],
    }  # a composite IR dict whose component is a Record's IR dict
    with pytest.raises(ValueError, match="Malformed composite component"):  # one type
        Expression.from_dict(record)  # read the malformed plan record


def _attr_record(attr: str) -> dict:
    """The IR record of ``Attr(attr)``."""
    return {"type": "expression", "op": "attr", "attr": attr}


@pytest.mark.parametrize(
    "record, message",
    [
        ({"op": "add", "right": 1}, "needs left and right"),
        ({"op": "add", "left": 1}, "needs left and right"),
        ({"op": "add", "left": "x", "right": 1}, "Malformed arithmetic operand"),
        ({"op": "add", "left": True, "right": 1}, "Malformed arithmetic operand"),
        (
            {"op": "add", "left": {"type": "predicate", "op": "gt"}, "right": 1},
            "Malformed arithmetic operand",
        ),
        ({"op": "record"}, "non-empty list of fields"),
        ({"op": "record", "fields": []}, "non-empty list of fields"),
        (
            {
                "op": "record",
                "fields": [
                    {"name": "a", "value": _attr_record("x")},
                    {"name": "a", "value": _attr_record("y")},
                ],
            },
            "Duplicate Record field",
        ),
        (
            {"op": "record", "fields": [{"name": "a.b", "value": _attr_record("x")}]},
            "Invalid Record field name",
        ),
        (
            {"op": "record", "fields": [{"name": "a", "value": 1}]},
            "Malformed Record field",
        ),
        (
            {
                "op": "record",
                "fields": [{"name": "a", "value": {"type": "predicate", "op": "gt"}}],
            },
            "Malformed Record field",
        ),
        (
            {
                "op": "record",
                "fields": [
                    {"name": "a", "value": Composite(Attr("x")).to_dict()},
                ],
            },
            "Malformed Record field",
        ),
    ],
    ids=[
        "arith-without-left",
        "arith-without-right",
        "arith-string-operand",
        "arith-bool-operand",
        "arith-predicate-operand",
        "record-without-fields",
        "record-empty-fields",
        "record-duplicate-name",
        "record-invalid-name",
        "record-constant-value",
        "record-predicate-without-fields",
        "record-composite-value",
    ],
)
def test_from_dict_rejects_malformed_arithmetic_or_record(
    record: dict, message: str
) -> None:
    """``Expression.from_dict`` raises ``ValueError`` for malformed 009 records.
    Would fail if a ``KeyError`` / ``TypeError`` leaked or a duplicate silently won."""
    with pytest.raises(ValueError, match=message):  # one clear error type
        Expression.from_dict({"type": "expression", **record})  # read the record
