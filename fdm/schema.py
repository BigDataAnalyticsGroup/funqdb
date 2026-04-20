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


from typing import Any, Callable, Type

from fdm.API import AttributeFunction
from fdm.attribute_functions import DictionaryAttributeFunction
from fql.predicates.constraints import AttributeFunctionConstraint
from fql.util import ChangeEvent
from store.store import Store


class Schema[Key](DictionaryAttributeFunction[Key, Type], AttributeFunctionConstraint):
    """A schema is an attribute function that defines the expected keys and their types for items in a relation."""

    def __init__(
        self,
        data=None,
        frozen=False,
        observe_items: bool = False,
        lineage: list[str] = None,
        store: Store = None,
    ):
        """Initialize a Schema with the given data and properties.
        @param data: A dictionary mapping keys to their expected types.
        @param frozen: Whether the schema is frozen (i.e., cannot be modified).
        @param observe_items: Whether to observe items for changes (not implemented).
        @param lineage: A list of strings representing the lineage of this schema (not implemented).
        @param store: A Store instance for managing this schema (not implemented).
        """
        super().__init__(
            data=data,
            frozen=frozen,
            observe_items=observe_items,
            lineage=lineage,
            store=store,
        )

    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        """Evaluates whether the given attribute_function fulfills the schema."""
        assert isinstance(attribute_function, AttributeFunction)

        # check if all keys in the schema are present in the attribute function and their types are compatible
        for item in attribute_function:
            if item.key not in self:
                return False
            if not isinstance(item.value, self[item.key]):
                return False
        return True

    def __hash__(self):
        """Compute the hash of the Schema based on its items.
        @return: The hash value of the Schema.
        """
        return AttributeFunction.__hash__(self)


class ForeignValueConstraint[Key](AttributeFunctionConstraint):
    """A foreign value constraint is an attribute function constraint that a given value of an attribute function must
    be mapped to by another attribute function (the target). This is used to express foreign value constraints between
    relations. This class is from the point of view of the referrer, i.e., the relation that has the foreign value
    reference to another relation.
    Note that is in contrast to relational DBMS that have foreign key constraints leading to an additional
    indirection. FDM does not require that indirection.
    """

    def __init__(self, key: Key, target_attribute_function: AttributeFunction):
        self.key = key
        self.target_attribute_function = target_attribute_function

    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        assert isinstance(attribute_function, AttributeFunction)

        # check whether the value mapped to by attribute_function[self.key] is available in the target attribute
        # function, i.e., whether there is an item in the target attribute function that maps to this value
        # O(n) find, TODO: replace by indexed version
        # maybe extend AFs to generally index on their values
        value_to_find = attribute_function[self.key]
        return (
            len(
                self.target_attribute_function.where(lambda i: i.value == value_to_find)
            )
            > 0
        )


class ReverseForeignObjectConstraint[Key](AttributeFunctionConstraint):
    """This is the reverse of a foreign value constraint, i.e., it is from the point of view of the referenced
    attribute function."""

    def __init__(self, key: Key, source_attribute_function: AttributeFunction):
        self.key = key
        self.source_attribute_function = source_attribute_function

    # TODO: actually this only has to be true in case we want to delete!
    # this will yield a constraint validation error if we do a change of the instance without deleting it, but that is
    # not what we want, so we need to distinguish between delete and update operations in the constraint check
    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        assert isinstance(attribute_function, AttributeFunction)
        # only relevant for delete events:
        return event != ChangeEvent.DELETE or (
            len(
                self.source_attribute_function.where(
                    lambda i: i.value[self.key] == attribute_function
                )
            )
            == 0
        )


class JoinPredicate(AttributeFunctionConstraint):
    """A cross-relation join predicate defined on a DBF.

    Unlike ForeignValueConstraint (a single-RF reference) this constraint
    spans two or more named relations in a DBF and encodes an arbitrary
    callable.
    It is meant to describe classical SQL-style join conditions
    (e.g. ``a.x < b.y``) that cannot be expressed through FDM object-
    identity references.
    Thus, this class covers both traditional binary but also
    n-ary join predicates (hyperedges) at the same time.

    Storage: added to the DBF's values_constraints via `add_values_constraint`
    (inherited from DictionaryAttributeFunction). Evaluation is deferred:
    `__call__` — the DBF-mutation hook — unconditionally returns True, so
    this constraint never blocks RF insertions or deletions (see its
    docstring for the reasoning). The actual predicate runs through
    `evaluate(tuples)`, which a downstream operator invokes once it has a
    tuple combination to evaluate against.

    Evaluation strategy in the flattening `join` operator (MR 2):

    * **Pushdown during the flatten walk.** `evaluate` is called as
      early as the walk can afford — as soon as all relations the
      predicate names are present in the current partial tuple
      combination. A predicate on ``("a", "b")`` fires the moment
      ``(a_tuple, b_tuple)`` is assembled, not after ``(a, b, c, d, …)``
      has been fully materialized. This is pushdown within the join
      operator, sound for every predicate shape, and requires no
      introspection of the callable.

    * **NOT consumed by the Yannakakis reduction.** `semijoin` and
      `subdatabase` follow only `ForeignValueConstraint` edges when
      building the reduction. Arbitrary callables cannot be
      introspected into semi-join keys, and the Yannakakis full-reducer
      property does not generalise to arbitrary θ-predicates. So the
      reduction stage ignores every `JoinPredicate`.

    * **Optional future path.** A structured equi-predicate on
      non-reference attributes (e.g. ``Eq("a.x", Ref("b.y"))`` where
      neither side is an FDM reference) *could* be lifted into an
      extra hash-semijoin step that extends the reference-driven
      Yannakakis reduction. That is a potential optimisation for a
      future MR, not part of MR 1 or MR 2. θ-predicates remain
      pushdown-during-walk only.

    The `description` field is an optional human-readable identifier that
    `drop_join_predicate` uses to match constraints across serialization
    boundaries (predicate callables are unreliable as identity keys when
    deserialized).
    """

    def __init__(
        self,
        relations: tuple[str, ...],
        predicate: Callable[..., bool],
        description: str | None = None,
    ):
        """Initialize a JoinPredicate.
        @param relations: Names of the relations this predicate spans. The join
            operator passes one tuple per named relation to `predicate` when
            evaluating a candidate combination.
        @param predicate: Callable invoked at join time with a TF wrapping
            the {relation_name: TF} mapping supplied by `evaluate`. Both
            `tuples["users"]` (dict-style) and `tuples.users.age` (getattr-
            style, used by structured predicates) work against that wrapper.
            Typed as `Callable[..., bool]` rather than a more specific
            signature so that lambdas, structured predicates from
            `fql.predicates.predicates`, and other callables fit uniformly.
        @param description: Optional human-readable identifier, used by
            `drop_join_predicate` for matching across serialization boundaries.
        """
        self.relations = tuple(relations)
        self.predicate = predicate
        self.description = description

    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        """Always returns True — intentional no-op at DBF-mutation time.

        `AttributeFunctionConstraint.__call__` is the hook that
        `DictionaryAttributeFunction._check_value_constraints` invokes on
        every DBF `__setitem__`/`__delitem__` — i.e. whenever an RF is
        added to or removed from the DBF. A JoinPredicate cannot be
        evaluated in that context: it spans N relations and needs one
        tuple *per relation* to decide, but at DBF-mutation time only a
        single freshly-inserted (or freshly-removed) RF is available —
        no tuple combination exists yet, so the question "does this RF
        satisfy the predicate?" is not semantically well-defined. The
        hook therefore returns True unconditionally, letting RFs move
        in and out of the DBF freely.

        Actual evaluation is delegated to `evaluate(tuples)`. See the
        class docstring for who calls it and when (post-filter in the
        flattening `join` operator, or — optionally — a semijoin
        condition in an extended Yannakakis reduction).
        """
        return True

    def evaluate(self, tuples: dict[str, Any]) -> bool:
        """Evaluate the predicate against one tuple per participating relation.

        The input dict is wrapped in a frozen TF before being handed to the
        predicate, so that structured predicates (e.g. `Gt("users.age",
        Ref("departments.min_age"))` from `fql.predicates.predicates`) can
        traverse relation.attribute paths through `getattr` the same way they
        do on ordinary tuple values. Plain `__getitem__` access works through
        the TF too, so lambda-style predicates that do
        `tuples["users"]["age"]` are unaffected.

        @param tuples: A dict {relation_name: TF}. The caller is expected to
            supply a tuple for every relation named in `self.relations`; the
            predicate itself is free to demand more or less. Missing entries
            surface as AttributeError when the predicate accesses them
            (TF.__getitem__ normalizes all unknown-key failures to
            AttributeError) — no pre-validation here to keep the evaluate
            path lean.
        @return: True iff the combination satisfies the predicate.
        """
        # local import to break the schema.py ↔ attribute_functions.py cycle
        from fdm.attribute_functions import TF

        wrapped: TF = TF(dict(tuples), frozen=True)
        return self.predicate(wrapped)
