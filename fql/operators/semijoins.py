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

from __future__ import annotations

from enum import Enum

from fdm.attribute_functions import RF, DBF
from fdm.schema import ForeignValueConstraint
from fql.operators.APIs import Operator, OperatorInput


class RefDirection(Enum):
    """Direction of a foreign-key reference between two RFs in a semijoin.

    REDUCE means the *reduce* RF is the source (holds the foreign key).
    BY means the *by* RF is the source (holds the foreign key).
    """

    REDUCE = "reduce"
    BY = "by"


def _has_reference(rf: RF, ref_key: str) -> bool:
    """Whether `rf` carries a ForeignValueConstraint on `ref_key` (any target).

    The target UUID is not checked: after semijoin chaining, reduced RFs get new
    UUIDs but inherit the original constraints, which still point at the
    unreduced relations.

    @param rf: The relation to inspect.
    @param ref_key: The referencing attribute name.
    @return: True iff `rf` holds a reference on `ref_key`.
    """
    return any(
        isinstance(constraint, ForeignValueConstraint) and constraint.key == ref_key
        for constraint in rf.__dict__["values_constraints"]
    )


def _find_ref_direction(reduce_rf: RF, by_rf: RF, ref_key: str) -> RefDirection:
    """Determine which RF is the source by inspecting ForeignValueConstraints.

    The source is the RF that holds a ForeignValueConstraint with the given
    ref_key (matched by name only, see `_has_reference`). Used only when the
    caller of `semijoin` does not name the source.

    @param reduce_rf: The relation to reduce.
    @param by_rf: The relation it is reduced by.
    @param ref_key: The referencing attribute name.
    @return: RefDirection.REDUCE if reduce_rf is the source (has ref_key),
        RefDirection.BY if by_rf is the source (has ref_key).
    @raises ValueError: If neither RF has the constraint, or if both have one:
        the name alone cannot tell which side is the source, so the caller must
        pass `source=` to `semijoin` instead of letting it guess.
    """
    reduce_has_reference: bool = _has_reference(reduce_rf, ref_key)
    by_has_reference: bool = _has_reference(by_rf, ref_key)
    if reduce_has_reference and by_has_reference:
        raise ValueError(
            f"Both relations carry a reference '{ref_key}', so the direction is "
            f"ambiguous. Pass source= to semijoin to name the relation whose "
            f"tuples hold '{ref_key}'."
        )
    if reduce_has_reference:
        return RefDirection.REDUCE
    if by_has_reference:
        return RefDirection.BY

    raise ValueError(
        f"No ForeignValueConstraint with ref_key='{ref_key}' found "
        f"between the two relations."
    )


class semijoin[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Semi-join operator: reduces one RF in a DBF by keeping only tuples that
    participate in a join with another RF in the same DBF.

    The join relationship is based on FDM references (set via .references()).
    In FDM, references are object identity: child_tuple[ref_key] ``is`` a TF
    in the target RF. The direction (which side is source, which is target) is
    given by `source`; if omitted, it is determined from the
    ForeignValueConstraint metadata, which fails if both relations carry a
    reference on `ref_key`.

    Input: DBF.  Output: DBF with the reduced relation replaced.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        reduce: str,
        by: str,
        ref_key: str,
        source: str | None = None,
        origin: str | None = None,
    ):
        """Initialize the semijoin operator.
        @param input_function: A DBF containing both relations involved in the semi-join.
        @param reduce: Name of the relation in the DBF to reduce (i.e. filter).
        @param by: Name of the relation in the DBF whose tuples decide which
            `reduce` tuples survive.
        @param ref_key: The attribute name that establishes the FDM reference between
            the two relations (set via .references()).
        @param source: Name of the relation whose tuples hold `ref_key` (either
            `reduce` or `by`). If omitted, it is determined from the
            ForeignValueConstraint metadata; that fails if both relations carry a
            reference on `ref_key`. `subdatabase` always passes it.
        @param origin: Optional tag indicating which target operator generated this
            semijoin (e.g. "subdatabase"). Surfaces in the extracted plan via
            _plan_params() so that generated semijoins are distinguishable from
            user-created ones.
        @raises ValueError: If `source` names neither `reduce` nor `by`.
        """
        if source is not None and source not in (reduce, by):
            raise ValueError(
                f"semijoin: source must be '{reduce}' (reduce) or '{by}' (by), "
                f"got '{source}'."
            )
        self.input_function = input_function
        self.reduce = reduce
        self.by = by
        self.ref_key = ref_key
        self.source = source
        self.origin = origin

    def _compute(self) -> OUTPUT_AttributeFunction:
        dbf: DBF = self._resolve_input(self.input_function)
        reduce_rf: RF = dbf[self.reduce]
        by_rf: RF = dbf[self.by]

        direction: RefDirection
        if self.source is None:
            direction = _find_ref_direction(reduce_rf, by_rf, self.ref_key)
        else:
            # the caller named the source; a source without a reference on
            # ref_key (e.g. a misspelt ref_key) is rejected instead of silently
            # emptying a relation
            direction = (
                RefDirection.REDUCE if self.source == self.reduce else RefDirection.BY
            )
            source_rf: RF = reduce_rf if direction is RefDirection.REDUCE else by_rf
            if not _has_reference(source_rf, self.ref_key):
                raise ValueError(
                    f"semijoin: No ForeignValueConstraint with "
                    f"ref_key='{self.ref_key}' found on source relation "
                    f"'{self.source}'."
                )

        reduced_rf: RF = RF(frozen=False)

        if direction is RefDirection.REDUCE:
            # reduce is source: reduce_tuple[ref_key] -> by (target)
            # keep reduce tuples whose ref_key points to a TF still in by_rf
            by_uuids: set[int] = {item.value.uuid for item in by_rf}
            for item in reduce_rf:
                # A source tuple whose ref_key is undefined has no reference, so
                # it points at no target and cannot survive the semijoin. Checked
                # by key membership (exception-free), not try/except.
                if (
                    self.ref_key in item.value
                    and item.value[self.ref_key].uuid in by_uuids
                ):
                    reduced_rf[item.key] = item.value
        elif direction is RefDirection.BY:
            # by is source: by_tuple[ref_key] -> reduce (target)
            # keep reduce tuples that are referenced by at least one by-tuple
            referenced_uuids: set[int] = set()
            for item in by_rf:
                # A by-source tuple with an undefined ref_key references nothing,
                # so it contributes no referenced target.
                if self.ref_key in item.value:
                    referenced_uuids.add(item.value[self.ref_key].uuid)
            for item in reduce_rf:
                if item.value.uuid in referenced_uuids:
                    reduced_rf[item.key] = item.value
        else:
            raise ValueError(f"Unknown direction '{direction}'")
        # copy constraints from original RF so chained semijoins can detect the
        # direction and check an explicit `source` on the reduced relation
        for constraint in reduce_rf.__dict__["values_constraints"]:
            reduced_rf.add_values_constraint(constraint)
        reduced_rf.freeze()

        # assemble output DBF: copy all relations, replace the reduced one
        output_dbf: DBF = DBF(frozen=False)
        for item in dbf:
            if item.key == self.reduce:
                output_dbf[item.key] = reduced_rf
            else:
                output_dbf[item.key] = item.value
        output_dbf.freeze()
        return output_dbf
