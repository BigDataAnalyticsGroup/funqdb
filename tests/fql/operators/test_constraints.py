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

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fdm.schema import (
    JoinPredicate,
    ForeignValueConstraint,
    ReverseForeignObjectConstraint,
)
from fql.operators.constraints import (
    add_join_predicate,
    add_reference,
    drop_join_predicate,
    drop_reference,
)
from fql.operators.semijoins import semijoin
from fql.predicates.predicates import Gt, Ref

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _raw_dept_users_dbf() -> tuple[DBF, RF, RF]:
    """Two-relation DBF without any references set — references must be
    added via add_reference in the tests."""
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Sales"}),
            "d3": TF({"name": "Bla"}),
        },
        frozen=True,
    )
    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": departments["d2"]}),
        },
        frozen=True,
    )
    dbf: DBF = DBF(
        {"departments": departments, "users": users},
        frozen=True,
    )
    return dbf, departments, users


def _preref_dept_users_dbf() -> tuple[DBF, RF, RF]:
    """Two-relation DBF with users.dept -> departments already set up via
    RF.references() — used to test drop_reference and rebinding behavior."""
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Sales"}),
        },
        frozen=False,
    )
    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": departments["d2"]}),
        },
        frozen=False,
    ).references("dept", departments)
    users.freeze()
    departments.freeze()
    dbf: DBF = DBF(
        {"departments": departments, "users": users},
        frozen=True,
    )
    return dbf, departments, users


def _three_relation_dbf() -> DBF:
    """DBF with tasks -> projects -> departments, no references set."""
    departments: RF = RF(
        {"d1": TF({"name": "Dev"})},
        frozen=True,
    )
    projects: RF = RF(
        {"p1": TF({"title": "Alpha", "dept": departments["d1"]})},
        frozen=True,
    )
    tasks: RF = RF(
        {"t1": TF({"desc": "Design", "project": projects["p1"]})},
        frozen=True,
    )
    return DBF(
        {"departments": departments, "projects": projects, "tasks": tasks},
        frozen=True,
    )


def _has_fvc(rf: RF, key: str) -> bool:
    """True if rf carries a ForeignValueConstraint with the given key."""
    return any(
        isinstance(c, ForeignValueConstraint) and c.key == key
        for c in rf.__dict__["values_constraints"]
    )


def _has_rfoc(rf: RF, key: str) -> bool:
    """True if rf carries a ReverseForeignObjectConstraint with the given key."""
    return any(
        isinstance(c, ReverseForeignObjectConstraint) and c.key == key
        for c in rf.__dict__["values_constraints"]
    )


def _predicate_constraints(dbf: DBF) -> list[JoinPredicate]:
    """All JoinPredicates currently registered on dbf."""
    return [
        c for c in dbf.__dict__["values_constraints"] if isinstance(c, JoinPredicate)
    ]


# ---------------------------------------------------------------------------
# add_reference
# ---------------------------------------------------------------------------


def test_add_reference_creates_foreign_value_constraint() -> None:
    """After add_reference, source RF has a FVC and target RF has an RFOC."""
    dbf, _, _ = _raw_dept_users_dbf()
    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    assert _has_fvc(out["users"], "dept"), "source RF must carry the FVC"
    assert _has_rfoc(out["departments"], "dept"), "target RF must carry the reverse"


def test_add_reference_preserves_original_dbf() -> None:
    """The input DBF and its RFs must NOT be mutated by add_reference."""
    dbf, departments, users = _raw_dept_users_dbf()
    _ = add_reference(dbf, source="users", ref_key="dept", target="departments").result

    assert not _has_fvc(users, "dept"), "original users RF must stay unchanged"
    assert not _has_rfoc(
        departments, "dept"
    ), "original departments RF must stay unchanged"


def test_add_reference_returns_new_dbf_identity() -> None:
    """The returned DBF is a fresh object, not the input DBF."""
    dbf, _, _ = _raw_dept_users_dbf()
    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result
    assert out is not dbf
    assert out["users"] is not dbf["users"]
    assert out["departments"] is not dbf["departments"]


def test_add_reference_missing_source_raises() -> None:
    """Unknown source relation raises ValueError at _compute."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match="source relation 'nope'"):
        _ = add_reference(
            dbf, source="nope", ref_key="dept", target="departments"
        ).result


def test_add_reference_missing_target_raises() -> None:
    """Unknown target relation raises ValueError at _compute."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match="target relation 'nowhere'"):
        _ = add_reference(dbf, source="users", ref_key="dept", target="nowhere").result


def test_add_reference_rejects_empty_strings() -> None:
    """Any empty argument is rejected at construction time."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match="non-empty"):
        add_reference(dbf, source="", ref_key="dept", target="departments")
    with pytest.raises(ValueError, match="non-empty"):
        add_reference(dbf, source="users", ref_key="", target="departments")
    with pytest.raises(ValueError, match="non-empty"):
        add_reference(dbf, source="users", ref_key="dept", target="")


def test_add_reference_rejects_non_dbf_input() -> None:
    """add_reference on a bare RF raises TypeError."""
    rf: RF = RF({"a": TF({"x": 1})}, frozen=True)
    with pytest.raises(TypeError, match="expects a DBF"):
        _ = add_reference(rf, source="users", ref_key="dept", target="x").result


def test_add_reference_enables_semijoin() -> None:
    """A reference added via add_reference is picked up by semijoin."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    reduced: DBF = semijoin[DBF, DBF](
        augmented, reduce="departments", by="users", ref_key="dept"
    ).result

    # Every department is referenced by at least one user — all survive.
    assert {item.key for item in reduced["departments"]} == {"d1", "d2"}


def test_add_reference_composes_with_semijoin() -> None:
    """add_reference output can be fed directly into semijoin (operator composition)."""
    dbf, _, _ = _raw_dept_users_dbf()
    reduced: DBF = semijoin[DBF, DBF](
        add_reference(dbf, source="users", ref_key="dept", target="departments"),
        reduce="departments",
        by="users",
        ref_key="dept",
    ).result
    assert {item.key for item in reduced["departments"]} == {"d1", "d2"}


def test_add_reference_rebinds_existing_references() -> None:
    """Pre-existing references survive add_reference and are rebound to clones.

    A chained add_reference that adds a *second* reference must not invalidate
    the first: the cloned RFs in the output DBF must still carry the existing
    FVC/RFOC, and those constraints must now point at the *new* RFs.
    """
    dbf = _three_relation_dbf()
    step1: DBF = add_reference(
        dbf, source="projects", ref_key="dept", target="departments"
    ).result
    step2: DBF = add_reference(
        step1, source="tasks", ref_key="project", target="projects"
    ).result

    assert _has_fvc(step2["projects"], "dept")
    assert _has_fvc(step2["tasks"], "project")

    fvc_tasks = next(
        c
        for c in step2["tasks"].__dict__["values_constraints"]
        if isinstance(c, ForeignValueConstraint) and c.key == "project"
    )
    assert fvc_tasks.target_attribute_function is step2["projects"]
    fvc_projects = next(
        c
        for c in step2["projects"].__dict__["values_constraints"]
        if isinstance(c, ForeignValueConstraint) and c.key == "dept"
    )
    assert fvc_projects.target_attribute_function is step2["departments"]


# ---------------------------------------------------------------------------
# add_join_predicate
# ---------------------------------------------------------------------------


def test_add_join_predicate_registers_on_dbf() -> None:
    """The DBF carries a JoinPredicate after add_join_predicate."""
    dbf, _, _ = _raw_dept_users_dbf()
    pred = lambda tuples: True
    out: DBF = add_join_predicate(dbf, "users", "departments", predicate=pred).result

    constraints = _predicate_constraints(out)
    assert len(constraints) == 1
    assert constraints[0].relations == ("users", "departments")
    assert constraints[0].predicate is pred


def test_add_join_predicate_preserves_description() -> None:
    """The description field is stored verbatim on the registered constraint."""
    dbf, _, _ = _raw_dept_users_dbf()
    out: DBF = add_join_predicate(
        dbf,
        "users",
        "departments",
        predicate=lambda t: True,
        description="users-depts-pred-v1",
    ).result

    constraint = _predicate_constraints(out)[0]
    assert constraint.description == "users-depts-pred-v1"


def test_add_join_predicate_missing_relation_raises() -> None:
    """A predicate naming a relation missing from the DBF raises ValueError."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match=r"relation\(s\) \['ghost'\]"):
        _ = add_join_predicate(dbf, "users", "ghost", predicate=lambda t: True).result


def test_add_join_predicate_requires_at_least_one_relation() -> None:
    """Call without any relation names is rejected at construction time."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match="at least one relation"):
        add_join_predicate(dbf, predicate=lambda t: True)


def test_add_join_predicate_rejects_non_callable() -> None:
    """Non-callable predicate is rejected at construction time."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(TypeError, match="must be callable"):
        add_join_predicate(
            dbf,
            "users",
            "departments",
            predicate="not-a-callable",  # type: ignore[arg-type]
        )


def test_add_join_predicate_not_evaluated_on_dbf_mutation() -> None:
    """Registering the constraint must not invoke the predicate.

    JoinPredicate is a join-time hook, not a DBF invariant. It must
    return True from __call__ so that freezing/adding RFs never triggers the
    user's predicate (which might have arbitrary side effects).
    """
    dbf, _, _ = _raw_dept_users_dbf()
    call_count: list[int] = [0]

    def tracking_predicate(_tuples) -> bool:
        # _tuples is accepted but unused — the whole point of the test is to
        # prove that DBF mutations don't call this at all.
        call_count[0] += 1
        return False

    out: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=tracking_predicate
    ).result

    # simulate a DBF-level constraint check by invoking the constraint manually
    constraint = _predicate_constraints(out)[0]
    from fql.util import ChangeEvent

    assert constraint(out["users"], ChangeEvent.UPDATE) is True
    assert (
        call_count[0] == 0
    ), "tracked predicate must never be called during DBF mutations"


def test_add_join_predicate_rejects_non_dbf_input() -> None:
    """add_join_predicate on a bare RF raises TypeError."""
    rf: RF = RF({"a": TF({"x": 1})}, frozen=True)
    with pytest.raises(TypeError, match="expects a DBF"):
        _ = add_join_predicate(rf, "a", predicate=lambda t: True).result


# ---------------------------------------------------------------------------
# JoinPredicate.evaluate — lambdas and structured predicates
# ---------------------------------------------------------------------------


def test_dbf_predicate_constraint_evaluate_runs_lambda_predicate() -> None:
    """evaluate() is the join-time entry point and must call the user predicate."""
    # Typed as list[TF] because JoinPredicate.evaluate wraps the
    # incoming dict in a TF before handing it to the predicate.
    tuple_log: list[TF] = []

    def predicate(tuples) -> bool:
        tuple_log.append(tuples)
        return tuples["a"]["x"] < tuples["b"]["y"]

    constraint: JoinPredicate = JoinPredicate(relations=("a", "b"), predicate=predicate)
    ta: TF = TF({"x": 1})
    tb: TF = TF({"y": 5})

    assert constraint.evaluate({"a": ta, "b": tb}) is True
    assert len(tuple_log) == 1
    # the predicate sees a TF wrapper, but tuples["a"] still returns the inner TF
    assert tuple_log[0]["a"] is ta and tuple_log[0]["b"] is tb


def test_dbf_predicate_constraint_evaluate_with_structured_predicate() -> None:
    """Structured predicates with Ref() for attribute-to-attribute comparisons
    work as join predicates through the TF wrapping in evaluate()."""
    # Gt("a.x", Ref("b.y")): read a.x and b.y from the wrapped input and
    # compare. TF wrapping makes relation.attribute path traversal succeed.
    constraint: JoinPredicate = JoinPredicate(
        relations=("a", "b"),
        predicate=Gt("a.x", Ref("b.y")),
    )
    assert constraint.evaluate({"a": TF({"x": 10}), "b": TF({"y": 5})}) is True
    assert constraint.evaluate({"a": TF({"x": 1}), "b": TF({"y": 5})}) is False


def test_dbf_predicate_constraint_evaluate_lambda_with_attribute_access() -> None:
    """A lambda that uses `tuples.a.x` attribute-style access (mirroring what
    structured predicates do internally) also works through the TF wrap.

    The tutorial page explicitly promises that both
    ``tuples["users"]["age"]`` (dict-style) and ``tuples.users.age``
    (getattr-style) work on the wrapper TF. The sibling test above covers
    the dict-style lambda path; this one locks in the getattr-style path
    for lambdas so a regression in TF's __getattr__ delegation surfaces
    here rather than only in a structured-predicate test.
    """
    constraint: JoinPredicate = JoinPredicate(
        relations=("a", "b"),
        predicate=lambda tuples: tuples.a.x < tuples.b.y,
    )
    assert constraint.evaluate({"a": TF({"x": 1}), "b": TF({"y": 5})}) is True
    assert constraint.evaluate({"a": TF({"x": 10}), "b": TF({"y": 5})}) is False


def test_add_join_predicate_accepts_structured_predicate() -> None:
    """add_join_predicate accepts a structured predicate as its predicate argument.

    The predicate is only registered here; real evaluation lands in the join
    operator (MR 2).
    """
    dbf, _, _ = _raw_dept_users_dbf()
    structured = Gt("users.id", Ref("departments.id"))
    out: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=structured, description="gt-demo"
    ).result
    constraint: JoinPredicate = _predicate_constraints(out)[0]
    assert constraint.predicate is structured


# ---------------------------------------------------------------------------
# drop_reference
# ---------------------------------------------------------------------------


def test_drop_reference_removes_both_sides() -> None:
    """Dropping a reference removes the FVC on source AND the RFOC on target."""
    dbf, _, _ = _preref_dept_users_dbf()
    assert _has_fvc(dbf["users"], "dept")
    assert _has_rfoc(dbf["departments"], "dept")

    out: DBF = drop_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    assert not _has_fvc(out["users"], "dept")
    assert not _has_rfoc(out["departments"], "dept")


def test_drop_reference_preserves_original_dbf() -> None:
    """Dropping in the operator must not mutate the input DBF."""
    dbf, departments, users = _preref_dept_users_dbf()
    _ = drop_reference(dbf, source="users", ref_key="dept", target="departments").result

    assert _has_fvc(users, "dept")
    assert _has_rfoc(departments, "dept")


def test_drop_reference_unmatched_ref_key_raises() -> None:
    """A ref_key that doesn't match any FVC raises ValueError (catches typos)."""
    dbf, _, _ = _preref_dept_users_dbf()
    with pytest.raises(ValueError, match="no ForeignValueConstraint"):
        _ = drop_reference(
            dbf, source="users", ref_key="nonexistent_key", target="departments"
        ).result


def test_drop_reference_missing_source_raises() -> None:
    """Unknown source relation raises ValueError."""
    dbf, _, _ = _preref_dept_users_dbf()
    with pytest.raises(ValueError, match="source relation 'nope'"):
        _ = drop_reference(
            dbf, source="nope", ref_key="dept", target="departments"
        ).result


def test_drop_reference_missing_target_raises() -> None:
    """Unknown target relation raises ValueError."""
    dbf, _, _ = _preref_dept_users_dbf()
    with pytest.raises(ValueError, match="target relation 'nowhere'"):
        _ = drop_reference(dbf, source="users", ref_key="dept", target="nowhere").result


def test_drop_reference_rejects_empty_strings() -> None:
    """Any empty argument is rejected at construction time."""
    dbf, _, _ = _preref_dept_users_dbf()
    with pytest.raises(ValueError, match="non-empty"):
        drop_reference(dbf, source="", ref_key="dept", target="departments")


def test_drop_reference_rejects_non_dbf_input() -> None:
    """drop_reference on a bare RF raises TypeError."""
    rf: RF = RF({"a": TF({"x": 1})}, frozen=True)
    with pytest.raises(TypeError, match="expects a DBF"):
        _ = drop_reference(rf, source="a", ref_key="x", target="y").result


def test_add_drop_reference_roundtrip() -> None:
    """add_reference followed by drop_reference for the same spec leaves no trace."""
    dbf, _, _ = _raw_dept_users_dbf()
    after_add: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result
    assert _has_fvc(after_add["users"], "dept")

    after_drop: DBF = drop_reference(
        after_add, source="users", ref_key="dept", target="departments"
    ).result
    assert not _has_fvc(after_drop["users"], "dept")
    assert not _has_rfoc(after_drop["departments"], "dept")


# ---------------------------------------------------------------------------
# drop_join_predicate
# ---------------------------------------------------------------------------


def test_drop_join_predicate_by_description() -> None:
    """description matches JoinPredicate by description."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        dbf,
        "users",
        "departments",
        predicate=lambda t: True,
        description="p1",
    ).result

    out: DBF = drop_join_predicate(augmented, description="p1").result
    assert _predicate_constraints(out) == []


def test_drop_join_predicate_by_description_no_match_raises() -> None:
    """Wrong description raises ValueError (catches typos)."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        dbf,
        "users",
        "departments",
        predicate=lambda t: True,
        description="p1",
    ).result

    with pytest.raises(ValueError, match="description='p2'"):
        _ = drop_join_predicate(augmented, description="p2").result


def test_drop_join_predicate_by_predicate_identity() -> None:
    """Predicate-identity match removes the constraint with that exact callable."""
    dbf, _, _ = _raw_dept_users_dbf()
    shared = lambda t: True
    augmented: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=shared
    ).result

    out: DBF = drop_join_predicate(augmented, predicate=shared).result
    assert _predicate_constraints(out) == []


def test_drop_join_predicate_by_predicate_identity_no_match_raises() -> None:
    """A different predicate object (same body) raises ValueError."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=lambda t: True
    ).result

    with pytest.raises(ValueError, match="predicate object identity"):
        _ = drop_join_predicate(
            augmented, predicate=lambda t: True  # different object
        ).result


def test_drop_join_predicate_via_matcher_no_match_is_silent() -> None:
    """Matcher mode is the 'maybe-nothing' escape hatch and does NOT raise
    when no constraint matches (unlike description / predicate-identity).

    The three drop modes encode different caller intents, and the no-match
    policy follows from that intent:

    * `description="p1"` and `predicate=p1_callable` both mean *"drop this
      specific constraint I know about"*. If nothing matches, the caller's
      mental model is wrong — typically a typo in the description or a
      stale handle on a callable that has since been replaced. Silently
      doing nothing would let such bugs slip through, so these two modes
      raise ValueError.

    * `matcher=lambda c: ...` means *"drop every constraint satisfying this
      rule, possibly none"*. Here a zero-match outcome is a legitimate
      result — for example when a pipeline tries to remove any predicate
      left over from an earlier branch but it is fine if none exist. This
      is the only mode that supports idempotent `drop_if_present`-style
      semantics, and it must therefore stay silent on no-match.

    This test locks in that contract: a matcher that never returns True
    produces a cloned DBF identical to the input (aside from RF identity)
    and does not raise, while the sibling tests above assert that
    description/predicate modes raise ValueError in the same situation.
    """
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=lambda t: True, description="p1"
    ).result

    # matcher returns False for everything → nothing dropped, no raise
    out: DBF = drop_join_predicate(augmented, matcher=lambda c: False).result
    assert len(_predicate_constraints(out)) == 1


def test_drop_join_predicate_via_matcher() -> None:
    """matcher callable removes every predicate for which it returns True."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        add_join_predicate(
            dbf,
            "users",
            "departments",
            predicate=lambda t: True,
            description="p1",
        ),
        "users",
        "departments",
        predicate=lambda t: False,
        description="p2",
    ).result
    assert len(_predicate_constraints(augmented)) == 2

    out: DBF = drop_join_predicate(augmented, matcher=lambda c: True).result
    assert _predicate_constraints(out) == []


def test_drop_join_predicate_via_matcher_selective() -> None:
    """matcher can drop only some constraints based on their description."""
    dbf, _, _ = _raw_dept_users_dbf()
    augmented: DBF = add_join_predicate(
        add_join_predicate(
            dbf,
            "users",
            "departments",
            predicate=lambda t: True,
            description="keep",
        ),
        "users",
        "departments",
        predicate=lambda t: False,
        description="drop",
    ).result

    out: DBF = drop_join_predicate(
        augmented, matcher=lambda c: c.description == "drop"
    ).result
    remaining = _predicate_constraints(out)
    assert len(remaining) == 1
    assert remaining[0].description == "keep"


def test_drop_join_predicate_requires_exactly_one_mode() -> None:
    """drop_join_predicate refuses if no mode or more than one is supplied."""
    dbf, _, _ = _raw_dept_users_dbf()
    with pytest.raises(ValueError, match="exactly one of"):
        drop_join_predicate(dbf)
    with pytest.raises(ValueError, match="only one of"):
        drop_join_predicate(dbf, description="x", matcher=lambda c: True)
    # also reject all three at once
    with pytest.raises(ValueError, match="only one of"):
        drop_join_predicate(
            dbf,
            description="x",
            predicate=lambda t: True,
            matcher=lambda c: True,
        )


def test_drop_join_predicate_rejects_non_dbf_input() -> None:
    """drop_join_predicate on a bare RF raises TypeError."""
    rf: RF = RF({"a": TF({"x": 1})}, frozen=True)
    with pytest.raises(TypeError, match="expects a DBF"):
        _ = drop_join_predicate(rf, description="x").result


# ---------------------------------------------------------------------------
# Clone preservation — ensures the clone does not silently drop RF/DBF state
# (Schema, af_constraints, computed, default/domain, external references).
# These guard against a regression of the clone helper using bare constructors.
# ---------------------------------------------------------------------------


def test_clone_preserves_schema_constraint() -> None:
    """A Schema values-constraint on an RF survives the clone."""
    from fdm.schema import Schema

    departments: RF = RF(
        {"d1": TF({"name": "Dev"})},
        frozen=False,
    )
    departments.add_values_constraint(Schema({"name": str}))
    departments.freeze()
    users: RF = RF({"u1": TF({"name": "Alice"})}, frozen=True)
    dbf: DBF = DBF({"departments": departments, "users": users}, frozen=True)

    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    # reminder:
    assert out is not dbf

    schema_constraints: list = [
        c
        for c in out["departments"].__dict__["values_constraints"]
        if isinstance(c, Schema)
    ]
    assert len(schema_constraints) == 1, (
        f"Schema constraint must survive the clone but found "
        f"{[type(c).__name__ for c in out['departments'].__dict__['values_constraints']]}"
    )


def test_clone_preserves_af_constraints() -> None:
    """AF-level constraints (af_constraints) survive the clone."""
    from fql.predicates.constraints import max_count

    departments: RF = RF({"d1": TF({"name": "Dev"})}, frozen=False)
    departments.add_attribute_function_constraint(max_count(10))
    departments.freeze()
    users: RF = RF({"u1": TF({"name": "Alice"})}, frozen=True)
    dbf: DBF = DBF({"departments": departments, "users": users}, frozen=True)

    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    assert any(
        isinstance(c, max_count) for c in out["departments"].__dict__["af_constraints"]
    ), "AF-level constraint must survive the clone"


def test_clone_preserves_computed_attributes() -> None:
    """Computed attributes (Sec 2.3) on an RF are reachable through the clone."""
    departments: RF = RF(
        {"d1": TF({"name": "Dev"})},
        frozen=False,
        computed={"tag": lambda rf: "constant-tag"},
    )
    departments.freeze()
    users: RF = RF({"u1": TF({"name": "Alice"})}, frozen=True)
    dbf: DBF = DBF({"departments": departments, "users": users}, frozen=True)

    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    assert out["departments"]["tag"] == "constant-tag"


def test_clone_preserves_default_and_domain() -> None:
    """A default callable and its active domain (Sec 2.4 / 2.6 of the paper) survive the clone."""
    departments: RF = RF(
        {"d1": TF({"name": "Dev"})},
        frozen=False,
        default=lambda key: TF({"name": f"generated-{key}"}),
        domain={"d1", "d2"},
    )
    departments.freeze()
    users: RF = RF({"u1": TF({"name": "Alice"})}, frozen=True)
    dbf: DBF = DBF({"departments": departments, "users": users}, frozen=True)

    out: DBF = add_reference(
        dbf, source="users", ref_key="dept", target="departments"
    ).result

    # "d2" is in the domain but not in data; the default must fire.
    assert out["departments"]["d2"]["name"] == "generated-d2"


def test_clone_preserves_external_reference() -> None:
    """A ForeignValueConstraint targeting an RF *outside* the DBF must survive
    the clone unchanged — it is not rebound because the target is not in the
    uuid→name map of the DBF being cloned.

    Relationship graph — what the fixture builds, then what the operator
    must produce:

        external_rf          (outside the DBF, never cloned)
            ^
            | FVC key="ext"
            |
        +-- users -------- other --+
        |                          |  DBF (input). No refs between
        +--------------------------+  users and other yet.

            |  add_reference(source="other",
            v                 ref_key="ref", target="users")

        external_rf          (same instance, never cloned)
            ^
            | FVC key="ext"   -- preserved VERBATIM: the target is
            |                    outside the DBF, so _rebind_constraint
            |                    leaves it alone.
            |
        +-- users' <---- FVC key="ref" ---- other' --+
        |                                             |  DBF' (clone)
        +---------------------------------------------+

    The assertion below locks in the pass-through branch of
    `_rebind_constraint`: `users'.ext` must still target `external_rf`
    by object identity, not a copy.
    """
    external_rf: RF = RF({"e1": TF({"tag": "external"})}, frozen=False)
    users: RF = RF(
        {"u1": TF({"name": "Alice", "ext": external_rf["e1"]})}, frozen=False
    ).references("ext", external_rf)
    users.freeze()
    external_rf.freeze()
    other: RF = RF({"x": TF({"y": 1})}, frozen=True)
    dbf: DBF = DBF({"users": users, "other": other}, frozen=True)

    out: DBF = add_reference(dbf, source="other", ref_key="ref", target="users").result

    ext_fvcs = [
        c
        for c in out["users"].__dict__["values_constraints"]
        if isinstance(c, ForeignValueConstraint) and c.key == "ext"
    ]
    assert len(ext_fvcs) == 1
    assert ext_fvcs[0].target_attribute_function is external_rf


# ---------------------------------------------------------------------------
# Plan extraction — operators should surface their params cleanly
# ---------------------------------------------------------------------------


def test_add_reference_plan_extraction() -> None:
    """to_plan() surfaces source/ref_key/target as plain plan parameters."""
    dbf, _, _ = _raw_dept_users_dbf()
    op = add_reference(dbf, source="users", ref_key="dept", target="departments")

    root = op.to_plan().root
    assert root.op == "add_reference"
    assert root.params["source"] == "users"
    assert root.params["ref_key"] == "dept"
    assert root.params["target"] == "departments"


def test_add_join_predicate_plan_extraction_with_lambda() -> None:
    """to_plan() surfaces relations and description as data; the lambda predicate
    becomes an Opaque marker with reason="lambda"."""
    from fql.plan.ir import Opaque

    dbf, _, _ = _raw_dept_users_dbf()
    op = add_join_predicate(
        dbf,
        "users",
        "departments",
        predicate=lambda t: True,
        description="demo",
    )

    root = op.to_plan().root
    assert root.op == "add_join_predicate"
    assert root.params["relations"] == ["users", "departments"]
    assert root.params["description"] == "demo"
    predicate_param = root.params["predicate"]
    assert isinstance(predicate_param, Opaque)
    assert predicate_param.reason == "lambda"


def test_add_join_predicate_plan_extraction_with_structured_predicate() -> None:
    """to_plan() surfaces structured predicates as structured objects, not Opaque."""
    from fql.predicates.predicates import Predicate

    dbf, _, _ = _raw_dept_users_dbf()
    structured = Gt("users.id", Ref("departments.id"))
    op = add_join_predicate(
        dbf, "users", "departments", predicate=structured, description="gt"
    )

    root = op.to_plan().root
    predicate_param = root.params["predicate"]
    # Predicate instances are passed through by _serialize_param, not turned
    # into Opaque markers — this is the whole point of using structured
    # predicates.
    assert isinstance(predicate_param, Predicate)
