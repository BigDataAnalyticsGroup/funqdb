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

"""Pickle-security regression tests for DictionaryAttributeFunction.

These tests pin down the behaviour described in KNOWN_BUGS.md H3:
``__getstate__`` filters constraint sets down to genuine
``AttributeFunctionConstraint`` instances on the write side, and
``__setstate__`` refuses to materialise an AF whose state still carries
disallowed constraint objects.

Together with the safe-unpickler proposed for C1, this closes the
deserialization-gadget channel where an attacker-supplied ``__reduce__``
on a "constraint" object would run during ``pickle.loads``.
"""

import pickle

import pytest

from fdm.attribute_functions import TF
from fql.predicates.constraints import (
    AttributeFunctionConstraint,
    attribute_name_equivalence,
)


def test_pickle_strips_bare_callable_from_values_constraints():
    """A bare callable (not an ``AttributeFunctionConstraint``) must be
    silently dropped on pickle — same convention as ``computed`` /
    ``default`` / ``domain``."""
    tf = TF({"name": "Alice"})  # construct a vanilla TF with one stored attribute
    bare_callable = lambda af, event: True  # a plain lambda — not a constraint subclass
    tf.__dict__["values_constraints"].add(
        bare_callable
    )  # bypass the typed setter to plant the bare callable
    assert (
        bare_callable in tf.__dict__["values_constraints"]
    )  # sanity-check the planting worked

    restored = pickle.loads(pickle.dumps(tf))  # round-trip through pickle

    assert (
        restored.__dict__["values_constraints"] == set()
    )  # the bare callable was stripped on write
    assert restored["name"] == "Alice"  # stored data is unaffected by the strip


def test_pickle_strips_bare_callable_from_af_constraints():
    """Same as the previous test but for the ``af_constraints`` slot, to
    pin down that both slots are filtered symmetrically."""
    tf = TF({"name": "Bob"})  # vanilla TF; content is irrelevant
    bare_callable = (
        lambda af, event: True
    )  # not an AttributeFunctionConstraint subclass
    tf.__dict__["af_constraints"].add(
        bare_callable
    )  # plant directly so we bypass the typed setter

    restored = pickle.loads(pickle.dumps(tf))  # round-trip

    assert restored.__dict__["af_constraints"] == set()  # stripped on the write side
    assert (
        restored.__dict__["values_constraints"] == set()
    )  # untouched slot is still empty


def test_pickle_preserves_legit_attribute_function_constraint():
    """Genuine ``AttributeFunctionConstraint`` subclasses must survive
    the round-trip — the filter is type-based, not blanket removal."""
    tf = TF({"name": "Alice", "yob": 1990})  # vanilla TF with two stored attrs
    legit_constraint = attribute_name_equivalence(
        {"name", "yob"}
    )  # a real AttributeFunctionConstraint subclass
    tf.add_values_constraint(
        legit_constraint
    )  # add via the public API to mirror real usage

    restored = pickle.loads(pickle.dumps(tf))  # round-trip

    assert (
        len(restored.__dict__["values_constraints"]) == 1
    )  # the legit constraint survived
    survivor = next(
        iter(restored.__dict__["values_constraints"])
    )  # pull it out for type/state checks
    assert isinstance(survivor, attribute_name_equivalence)  # it kept its class
    assert survivor.attribute_names == {"name", "yob"}  # its state survived intact


def test_pickle_neutralises_reduce_gadget_in_values_constraints():
    """The actual H3 attack: an object with a malicious ``__reduce__``
    placed in ``values_constraints``. With the fix, the gadget is
    filtered out by ``__getstate__`` so its ``__reduce__`` is never
    consulted by the pickler, and consequently never runs on
    unpickle."""
    triggered: list[str] = (
        []
    )  # observable side-channel; populated only if the gadget runs

    class Pwn:  # deliberately NOT a subclass of AttributeFunctionConstraint
        def __call__(self, af, event):  # satisfy the duck-typed constraint protocol
            return True

        def __reduce__(
            self,
        ):  # this is the gadget — would execute on unpickle if it reached the stream
            return (
                triggered.append,
                ("payload-ran",),
            )  # benign payload, observable via the list

    tf = TF({"name": "Alice"})  # vanilla TF
    tf.__dict__["values_constraints"].add(
        Pwn()
    )  # plant the gadget, bypassing the typed setter

    blob = pickle.dumps(
        tf
    )  # __getstate__ should strip the gadget before it is reachable
    assert triggered == []  # the gadget's __reduce__ was not called during dumps

    restored = pickle.loads(blob)  # unpickle the cleaned blob
    assert triggered == []  # no payload ran on load either — gadget is not in the bytes
    assert (
        restored.__dict__["values_constraints"] == set()
    )  # and the constraint slot is empty


def test_setstate_rejects_disallowed_object_in_values_constraints():
    """Defense in depth: even when ``__setstate__`` is reached with a
    hand-crafted state (bypassing ``__getstate__``), it must refuse to
    materialise the AF."""
    tf = TF.__new__(TF)  # bypass __init__ to obtain a bare TF shell
    bad_state = {  # the kind of state a hand-crafted pickle would inject
        "data": {},  # required by other code paths but irrelevant here
        "frozen": False,  # not relevant for __setstate__ itself
        "af_constraints": set(),  # this slot is fine
        "values_constraints": {
            object()
        },  # an instance whose class is not an AttributeFunctionConstraint
        "observers": [],  # empty observer list
        "lineage": [],  # empty lineage
        "store": None,  # no store reference
        "computed": {},  # empty computed dict
        "default": None,  # no default fallback
        "domain": None,  # no domain
        "observe_items": False,  # consistent with __init__ default
    }

    with pytest.raises(
        pickle.UnpicklingError, match="values_constraints"
    ):  # the defensive check must fire
        tf.__setstate__(bad_state)  # this should raise before mutating self.__dict__


def test_setstate_rejects_disallowed_object_in_af_constraints():
    """Symmetric defense-in-depth check for the ``af_constraints`` slot."""
    tf = TF.__new__(TF)  # bare TF shell
    bad_state = {  # same shape as above but the gadget sits in af_constraints
        "data": {},  # placeholder
        "frozen": False,  # placeholder
        "af_constraints": {object()},  # disallowed entry
        "values_constraints": set(),  # this slot is clean
        "observers": [],  # placeholder
        "lineage": [],  # placeholder
        "store": None,  # placeholder
        "computed": {},  # placeholder
        "default": None,  # placeholder
        "domain": None,  # placeholder
        "observe_items": False,  # placeholder
    }

    with pytest.raises(
        pickle.UnpicklingError, match="af_constraints"
    ):  # message must point at the right slot
        tf.__setstate__(bad_state)  # state has not been applied yet


def test_setstate_accepts_legit_constraint_in_state():
    """Round-trip the defensive ``__setstate__`` with a legitimate
    ``AttributeFunctionConstraint`` instance — it must not be rejected
    by the new validator."""
    tf = TF.__new__(TF)  # bare shell
    legit = attribute_name_equivalence(
        {"name"}
    )  # legit AttributeFunctionConstraint subclass
    good_state = {  # state shaped like what __getstate__ would emit
        "data": {},  # placeholder
        "frozen": False,  # placeholder
        "af_constraints": set(),  # empty
        "values_constraints": {legit},  # contains an allowed type
        "observers": [],  # placeholder
        "lineage": [],  # placeholder
        "store": None,  # placeholder
        "computed": {},  # placeholder
        "default": None,  # placeholder
        "domain": None,  # placeholder
        "observe_items": False,  # placeholder
    }

    tf.__setstate__(good_state)  # must succeed without raising

    assert tf.__dict__["values_constraints"] == {
        legit
    }  # the legit constraint is on the instance
    assert isinstance(  # and is still an AttributeFunctionConstraint
        next(iter(tf.__dict__["values_constraints"])), AttributeFunctionConstraint
    )
