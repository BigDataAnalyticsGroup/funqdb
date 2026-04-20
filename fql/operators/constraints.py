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

"""FQL operators for managing constraints on a DBF.

Separates join-spec configuration (references, predicate constraints) from
join-spec execution: the `join` operator (separate MR) expects a DBF whose
constraints fully describe the join. Four specialized operators let users
assemble that DBF in a pipeline-friendly, plan-extractable way:

    add_reference      / drop_reference       — ForeignValueConstraint + reverse
    add_join_predicate / drop_join_predicate  — JoinPredicate

All four operators return a new DBF with freshly cloned RFs; the input DBF
and its RFs are never mutated. Intra-DBF references (ForeignValueConstraint
and ReverseForeignObjectConstraint pointing to other relations in the same
DBF) are re-bound to the cloned RFs so the returned DBF is self-consistent.

Join predicates can be given as ordinary Python callables (``lambda``) or as
structured predicates from ``fql.predicates.predicates`` (``Eq``, ``Gt``,
``And``, …, with ``Ref`` for attribute-to-attribute comparisons). Structured
predicates survive plan extraction as structured dicts instead of Opaque
markers, which is the preferred form.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

from fdm.attribute_functions import RF, DBF
from fdm.schema import (
    JoinPredicate,
    ForeignValueConstraint,
    ReverseForeignObjectConstraint,
)
from fql.operators.APIs import Operator, OperatorInput
from fql.predicates.constraints import AttributeFunctionConstraint

# ---------------------------------------------------------------------------
# Internals: clone + rebind
# ---------------------------------------------------------------------------


# Filters returning True mean "drop this constraint" (skip copying it into the
# clone). Default is "keep everything". The RF-level filter additionally
# receives the relation name and the old_uuid_to_name map so it can correlate
# a constraint's target/source AttributeFunction with its DBF-relation name.
_DropRFConstraint = Callable[
    [str, AttributeFunctionConstraint, Mapping[int, str]], bool
]
_DropDBFConstraint = Callable[[AttributeFunctionConstraint], bool]


def _clone_dbf_rebinding_refs(
    dbf: DBF,
    *,
    drop_rf_constraint: _DropRFConstraint | None = None,
    drop_dbf_constraint: _DropDBFConstraint | None = None,
) -> tuple[DBF, dict[str, RF]]:
    """Clone a DBF into a new DBF, re-binding intra-DBF references.

    Each RF is cloned via its `.copy()` method so all state is preserved
    (data, computed, default, domain, store, lineage, af_constraints,
    observers, values_constraints). The clone gets a fresh UUID. The clone's
    `data` dict is shallow-copied by `.copy()`, so contained TFs are shared
    with the original — this preserves FDM object-identity semantics for
    reference walks.

    `values_constraints` on the clones are then rebuilt from the originals:
    any ForeignValueConstraint / ReverseForeignObjectConstraint whose
    counterpart is also in the DBF is rewritten to point at the cloned
    counterpart, so the returned DBF is internally consistent. External
    references (pointing outside the DBF) are preserved verbatim. Other
    constraint types (Schema, attribute_name_equivalence, …) are carried
    forward unchanged.

    @param dbf: Source DBF. Iterated via the public protocol so computed
        relations (if any) are included — consistent with semijoin/subdatabase.
    @param drop_rf_constraint: Optional filter (name, constraint,
        old_uuid_to_name) → True to skip copying that constraint into the
        cloned RF.
    @param drop_dbf_constraint: Optional filter (constraint) → True to skip
        copying that DBF-level constraint. DBF-level constraints are not
        currently rebound (only `JoinPredicate` lives here today and
        it holds only relation names, no AF pointers).
    @return: (new DBF, {name -> new RF}). The DBF and its RFs are returned
        unfrozen; callers are expected to freeze them after any further
        mutations (add_values_constraint etc.) that they perform.
    """
    name_to_old: dict[str, RF] = {item.key: item.value for item in dbf}
    old_uuid_to_name: dict[int, str] = {
        rf.uuid: name for name, rf in name_to_old.items()
    }

    # Step 1: clone each RF via .copy() (fresh UUID, all state preserved),
    # then reset values_constraints so we can rebuild them with rebinding
    # and drop-filtering below. Observers are dropped from the clone so that
    # notification chains from the input DBF do not survive into the clone
    # (defense in depth — no mutation path currently fires observers during
    # _compute, but the reset prevents future accidents).
    name_to_new: dict[str, RF] = {}
    for name, old_rf in name_to_old.items():
        new_rf: RF = old_rf.copy()
        new_rf.__dict__["frozen"] = False
        new_rf.__dict__["values_constraints"] = set()
        new_rf.__dict__["observers"] = []
        name_to_new[name] = new_rf

    # Step 2: copy each constraint, rebinding intra-DBF pointers to the clones.
    for name, old_rf in name_to_old.items():
        new_rf = name_to_new[name]
        for constraint in old_rf.__dict__["values_constraints"]:
            if drop_rf_constraint is not None and drop_rf_constraint(
                name, constraint, old_uuid_to_name
            ):
                continue
            new_rf.add_values_constraint(
                _rebind_constraint(constraint, old_uuid_to_name, name_to_new)
            )

    # Step 3: clone the DBF via .copy() (preserves DBF-level state too — stores,
    # af_constraints, lineage, etc.), retarget its data dict to the cloned RFs,
    # and rebuild DBF-level values_constraints under the drop filter.
    new_dbf: DBF = dbf.copy()
    new_dbf.__dict__["frozen"] = False
    new_dbf.__dict__["values_constraints"] = set()
    new_dbf.__dict__["observers"] = []
    new_dbf.__dict__["data"] = dict(name_to_new)
    for constraint in dbf.__dict__["values_constraints"]:
        if drop_dbf_constraint is not None and drop_dbf_constraint(constraint):
            continue
        new_dbf.add_values_constraint(constraint)

    return new_dbf, name_to_new


def _rebind_constraint(
    constraint: AttributeFunctionConstraint,
    old_uuid_to_name: Mapping[int, str],
    name_to_new: Mapping[str, RF],
) -> AttributeFunctionConstraint:
    """Return a constraint pointing at cloned RFs instead of the originals.

    Only ForeignValueConstraint and ReverseForeignObjectConstraint hold
    cross-RF references worth rebinding. Other constraints (Schema,
    attribute_name_equivalence, etc.) are returned unchanged.

    If a reference points outside the DBF (its target/source UUID is not in
    `old_uuid_to_name`), the constraint is returned unchanged — the external
    reference survives the clone verbatim. Callers should treat these
    shared-constraint instances as effectively immutable, because they are
    shared between input and output DBF.
    """
    if isinstance(constraint, ForeignValueConstraint):
        target_uuid: int = constraint.target_attribute_function.uuid
        if target_uuid in old_uuid_to_name:
            new_target: RF = name_to_new[old_uuid_to_name[target_uuid]]
            return ForeignValueConstraint(constraint.key, new_target)
    elif isinstance(constraint, ReverseForeignObjectConstraint):
        source_uuid: int = constraint.source_attribute_function.uuid
        if source_uuid in old_uuid_to_name:
            new_source: RF = name_to_new[old_uuid_to_name[source_uuid]]
            return ReverseForeignObjectConstraint(constraint.key, new_source)
    return constraint


def _require_dbf(dbf: Any, operator_name: str) -> DBF:
    """Runtime downcast guard. Raises TypeError if the input is not a DBF.
    Kept as a single helper so the error message phrasing stays consistent.
    """
    if not isinstance(dbf, DBF):
        raise TypeError(
            f"{operator_name} expects a DBF input, got {type(dbf).__name__}"
        )
    return dbf


def _freeze_result(dbf: DBF, rfs: Mapping[str, RF]) -> DBF:
    """Freeze every cloned RF and the DBF itself before returning."""
    for rf in rfs.values():
        rf.freeze()
    dbf.freeze()
    return dbf


# ---------------------------------------------------------------------------
# References — add / drop
# ---------------------------------------------------------------------------


class add_reference[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Add a cross-relation reference (FK) to a DBF.

    Installs a `ForeignValueConstraint(ref_key, target_rf)` on the cloned
    source RF and a `ReverseForeignObjectConstraint(ref_key, source_rf)` on
    the cloned target RF — the same effect as the eager `RF.references()`
    method, but on clones so the input DBF stays intact.

    Returns a new DBF with cloned RFs.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        source: str,
        ref_key: str,
        target: str,
    ):
        """Initialize the add_reference operator.
        @param input_function: A DBF (or an Operator producing one).
        @param source: Name of the relation holding the reference key.
        @param ref_key: The attribute on `source` whose value points at a
            tuple in `target`.
        @param target: Name of the relation being referenced.
        @raises ValueError: If any of source/ref_key/target is empty.
        """
        if not source or not ref_key or not target:
            raise ValueError(
                f"add_reference: source, ref_key and target must all be "
                f"non-empty; got source={source!r}, ref_key={ref_key!r}, "
                f"target={target!r}"
            )
        self.input_function = input_function
        self.source = source
        self.ref_key = ref_key
        self.target = target

    def _compute(self) -> DBF:
        dbf: DBF = _require_dbf(
            self._resolve_input(self.input_function), "add_reference"
        )
        new_dbf, name_to_new = _clone_dbf_rebinding_refs(dbf)

        if self.source not in name_to_new:
            raise ValueError(
                f"add_reference: source relation '{self.source}' is not in "
                f"the DBF. Available: {sorted(name_to_new)}"
            )
        if self.target not in name_to_new:
            raise ValueError(
                f"add_reference: target relation '{self.target}' is not in "
                f"the DBF. Available: {sorted(name_to_new)}"
            )
        source_rf: RF = name_to_new[self.source]
        target_rf: RF = name_to_new[self.target]
        source_rf.add_values_constraint(ForeignValueConstraint(self.ref_key, target_rf))
        target_rf.add_values_constraint(
            ReverseForeignObjectConstraint(self.ref_key, source_rf)
        )

        return _freeze_result(new_dbf, name_to_new)


class drop_reference[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Remove a cross-relation reference from a DBF.

    Drops the `ForeignValueConstraint` on the named source RF and the
    matching `ReverseForeignObjectConstraint` on the named target RF
    (both scoped to the given ref_key). Non-matching constraints are
    preserved and re-bound to the cloned RFs where needed.

    Raises `ValueError` if the input DBF does not contain `source`,
    does not contain `target`, or does not carry a matching FVC — the
    operator refuses to be a silent no-op so typos in relation/key
    names surface immediately. If you need idempotent "drop if
    present" semantics, use `drop_join_predicate`'s matcher mode for
    predicates or add a dedicated matcher-mode variant here later.

    Returns a new DBF with cloned RFs.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        source: str,
        ref_key: str,
        target: str,
    ):
        """Initialize the drop_reference operator.
        @param input_function: A DBF (or an Operator producing one).
        @param source: Name of the relation whose FVC should be dropped.
        @param ref_key: The attribute identifying the reference.
        @param target: Name of the relation whose reverse RFOC should be
            dropped.
        @raises ValueError: If any of source/ref_key/target is empty.
        """
        if not source or not ref_key or not target:
            raise ValueError(
                f"drop_reference: source, ref_key and target must all be "
                f"non-empty; got source={source!r}, ref_key={ref_key!r}, "
                f"target={target!r}"
            )
        self.input_function = input_function
        self.source = source
        self.ref_key = ref_key
        self.target = target

    def _compute(self) -> DBF:
        dbf: DBF = _require_dbf(
            self._resolve_input(self.input_function), "drop_reference"
        )

        # Validate relation names against the DBF's stored data view
        # (not __iter__) so that exotic DBFs with a default callable or
        # computed relations don't fire their generators during validation.
        # A DBF whose relations are solely computed/default-backed is out
        # of scope for constraint manipulation and would fail here — that
        # matches the spirit of the paper's subdatabase semantics.
        data: dict[str, RF] = dbf.__dict__["data"]
        if self.source not in data:
            raise ValueError(
                f"drop_reference: source relation '{self.source}' is not "
                f"in the DBF. Available: {sorted(data)}"
            )
        if self.target not in data:
            raise ValueError(
                f"drop_reference: target relation '{self.target}' is not "
                f"in the DBF. Available: {sorted(data)}"
            )

        # Validate that a matching FVC actually exists on source → target
        # with the given ref_key.
        source_rf: RF = data[self.source]
        target_uuid: int = data[self.target].uuid
        has_match: bool = any(
            isinstance(c, ForeignValueConstraint)
            and c.key == self.ref_key
            and c.target_attribute_function.uuid == target_uuid
            for c in source_rf.__dict__["values_constraints"]
        )
        if not has_match:
            raise ValueError(
                f"drop_reference: no ForeignValueConstraint with ref_key="
                f"'{self.ref_key}' from '{self.source}' to '{self.target}' "
                f"found in the DBF."
            )

        drop_filter: _DropRFConstraint = _make_drop_reference_filter(
            source=self.source, ref_key=self.ref_key, target=self.target
        )
        new_dbf, name_to_new = _clone_dbf_rebinding_refs(
            dbf, drop_rf_constraint=drop_filter
        )
        return _freeze_result(new_dbf, name_to_new)


def _make_drop_reference_filter(
    *, source: str, ref_key: str, target: str
) -> _DropRFConstraint:
    """Closure that drops the FVC on source and the RFOC on target."""

    def drop(
        name: str,
        constraint: AttributeFunctionConstraint,
        old_uuid_to_name: Mapping[int, str],
    ) -> bool:
        if name == source and isinstance(constraint, ForeignValueConstraint):
            if constraint.key != ref_key:
                return False
            return (
                old_uuid_to_name.get(constraint.target_attribute_function.uuid)
                == target
            )
        if name == target and isinstance(constraint, ReverseForeignObjectConstraint):
            if constraint.key != ref_key:
                return False
            return (
                old_uuid_to_name.get(constraint.source_attribute_function.uuid)
                == source
            )
        return False

    return drop


# ---------------------------------------------------------------------------
# Join predicates — add / drop
# ---------------------------------------------------------------------------


class add_join_predicate[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Add an arbitrary cross-relation predicate to a DBF.

    Registers a `JoinPredicate` on the cloned DBF. The predicate may
    be a plain callable (``lambda``) or a structured predicate from
    ``fql.predicates.predicates`` (e.g. ``Gt("a.x", Ref("b.y"))``).

    When the registered predicate fires:

    * `semijoin` and `subdatabase` never consume it — they follow only
      `ForeignValueConstraint` edges during the Yannakakis reduction.
    * The forthcoming flattening `join` operator (MR 2) evaluates it
      with pushdown during its tuple walk: as soon as every relation
      the predicate names is present in the current partial
      combination, `JoinPredicate.evaluate` is called to decide
      whether the combination survives. That is an early prune
      within the join, not a single post-materialization pass.
    * Passing a side-effecting predicate is safe — it is never
      invoked during DBF mutations or during the Yannakakis
      reduction; see `JoinPredicate.__call__` and its class docstring
      for the full contract.

    Relations are positional so the call site reads like
    ``add_join_predicate(dbf, "users", "departments", predicate=...)``.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *relations: str,
        predicate: Callable[..., bool],
        description: str | None = None,
    ):
        """Initialize the add_join_predicate operator.
        @param input_function: A DBF (or an Operator producing one).
        @param relations: Names of the relations the predicate spans (>= 1).
        @param predicate: Callable invoked at join time. Receives a TF that
            wraps the {relation_name: TF} mapping (so both ``tuples["users"]``
            and ``tuples.users.age`` style access work). Accepts any callable
            with that single-argument signature, including structured
            predicates like ``Eq``/``Gt``/etc.
        @param description: Optional human-readable identifier used by
            ``drop_join_predicate`` to match across serialization boundaries.
        @raises ValueError: If `relations` is empty.
        @raises TypeError: If `predicate` is not callable.
        """
        if not relations:
            raise ValueError(
                "add_join_predicate: at least one relation name is required."
            )
        if not callable(predicate):
            raise TypeError(
                f"add_join_predicate: predicate must be callable, "
                f"got {type(predicate).__name__}"
            )
        self.input_function = input_function
        self.relations: tuple[str, ...] = tuple(relations)
        self.predicate = predicate
        self.description = description

    def _compute(self) -> DBF:
        dbf: DBF = _require_dbf(
            self._resolve_input(self.input_function), "add_join_predicate"
        )
        new_dbf, name_to_new = _clone_dbf_rebinding_refs(dbf)

        missing: list[str] = [r for r in self.relations if r not in name_to_new]
        if missing:
            raise ValueError(
                f"add_join_predicate: relation(s) {missing} are not in the "
                f"DBF. Available: {sorted(name_to_new)}"
            )

        new_dbf.add_values_constraint(
            JoinPredicate(
                relations=self.relations,
                predicate=self.predicate,
                description=self.description,
            )
        )

        return _freeze_result(new_dbf, name_to_new)


class drop_join_predicate[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Remove `JoinPredicate`s from a DBF.

    Exactly one of three matching modes must be supplied:

    * `description`: match by description string. Preferred across
      serialization boundaries (lambdas cannot be identity-compared after a
      round trip). **Raises ValueError** if no JoinPredicate with that
      description exists — catches typos.
    * `predicate`: match by predicate object identity. Useful when the caller
      still has a handle on the exact predicate they registered.
      **Raises ValueError** if no JoinPredicate holds that exact object.
    * `matcher`: arbitrary callable(JoinPredicate) -> bool. Useful for bulk
      drops, e.g. ``matcher=lambda c: c.relations == ("a", "b")``, and the
      explicit "maybe-nothing" escape hatch — **does not raise** when it
      matches zero constraints.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        description: str | None = None,
        predicate: Callable[..., bool] | None = None,
        matcher: Callable[[JoinPredicate], bool] | None = None,
    ):
        """Initialize the drop_join_predicate operator.
        @param input_function: A DBF (or an Operator producing one).
        @param description: Drop any JoinPredicate whose
            `description` equals this value.
        @param predicate: Drop any JoinPredicate whose `predicate`
            attribute is the same Python object.
        @param matcher: Drop any JoinPredicate for which the
            callable returns True.
        @raises ValueError: If none or more than one of the three modes is
            supplied.
        """
        modes: list[str] = [
            name
            for name, value in (
                ("description", description),
                ("predicate", predicate),
                ("matcher", matcher),
            )
            if value is not None
        ]
        if len(modes) == 0:
            raise ValueError(
                "drop_join_predicate requires exactly one of "
                "`description`, `predicate`, or `matcher`."
            )
        if len(modes) > 1:
            raise ValueError(
                f"drop_join_predicate takes only one of "
                f"`description`/`predicate`/`matcher`, got multiple: "
                f"{modes}."
            )
        self.input_function = input_function
        self.description = description
        self.predicate = predicate
        self.matcher = matcher

    def _compute(self) -> DBF:
        dbf: DBF = _require_dbf(
            self._resolve_input(self.input_function), "drop_join_predicate"
        )

        # In description / predicate-identity modes, refuse to be a silent
        # no-op — surface typos and stale references immediately. Matcher
        # mode is the explicit "drop whatever matches, possibly nothing"
        # escape hatch and stays silent.
        if self.matcher is None:
            joins: list[JoinPredicate] = [
                c
                for c in dbf.__dict__["values_constraints"]
                if isinstance(c, JoinPredicate)
            ]
            if self.description is not None:
                if not any(p.description == self.description for p in joins):
                    raise ValueError(
                        f"drop_join_predicate: no JoinPredicate with "
                        f"description={self.description!r} found in the DBF."
                    )
            else:  # predicate-identity mode — enforced by __init__
                if not any(p.predicate is self.predicate for p in joins):
                    raise ValueError(
                        "drop_join_predicate: no JoinPredicate matching the "
                        "given predicate object identity found in the DBF."
                    )

        drop_filter: _DropDBFConstraint = self._build_drop_filter()
        new_dbf, name_to_new = _clone_dbf_rebinding_refs(
            dbf, drop_dbf_constraint=drop_filter
        )
        return _freeze_result(new_dbf, name_to_new)

    def _build_drop_filter(self) -> _DropDBFConstraint:
        """Build the DBF-level drop filter from the configured matching mode."""
        if self.matcher is not None:
            m: Callable[[JoinPredicate], bool] = self.matcher

            def drop_via_matcher(c: AttributeFunctionConstraint) -> bool:
                return isinstance(c, JoinPredicate) and m(c)

            return drop_via_matcher

        if self.description is not None:
            description: str = self.description

            def drop_via_description(c: AttributeFunctionConstraint) -> bool:
                return isinstance(c, JoinPredicate) and c.description == description

            return drop_via_description

        # Predicate-identity mode — __init__ has already enforced that exactly
        # one of the three modes is set, so self.predicate is non-None here.
        assert self.predicate is not None
        target_predicate: Callable[..., bool] = self.predicate

        def drop_via_predicate_identity(c: AttributeFunctionConstraint) -> bool:
            return isinstance(c, JoinPredicate) and c.predicate is target_predicate

        return drop_via_predicate_identity
