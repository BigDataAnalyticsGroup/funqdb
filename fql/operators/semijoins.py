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


def _find_ref_direction(reduce_rf: RF, by_rf: RF, ref_key: str) -> RefDirection:
    """Determine which RF is the source by inspecting ForeignValueConstraints.

    The source is the RF that holds a ForeignValueConstraint with the given ref_key.
    The target UUID is not checked — ref_key alone is sufficient to identify the source
    side, and this makes the function robust after semijoin chaining where reduced RFs
    get new UUIDs but inherit the original constraints.

    Returns RefDirection.REDUCE if reduce_rf is the source (has ref_key),
    or RefDirection.BY if by_rf is the source (has ref_key).
    Raises ValueError if neither RF has the constraint.
    """
    for constraint in reduce_rf.__dict__["values_constraints"]:
        if isinstance(constraint, ForeignValueConstraint) and constraint.key == ref_key:
            return RefDirection.REDUCE

    for constraint in by_rf.__dict__["values_constraints"]:
        if isinstance(constraint, ForeignValueConstraint) and constraint.key == ref_key:
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
    determined automatically from the ForeignValueConstraint metadata.

    Input: DBF.  Output: DBF with the reduced relation replaced.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        reduce: str,
        by: str,
        ref_key: str,
        origin: str | None = None,
    ):
        """Initialize the semijoin operator.
        @param input_function: A DBF containing both relations involved in the semi-join.
        @param reduce: Name of the relation in the DBF to reduce (i.e. filter).
        @param by: Name of the relation in the DBF used as the filter source.
        @param ref_key: The attribute name that establishes the FDM reference between
            the two relations (set via .references()). The direction (which relation
            is the source holding ref_key, which is the target being referenced) is
            determined automatically from the ForeignValueConstraint metadata.
        @param origin: Optional tag indicating which target operator generated this
            semijoin (e.g. "subdatabase"). Surfaces in the extracted plan via
            _plan_params() so that generated semijoins are distinguishable from
            user-created ones.
        """
        self.input_function = input_function
        self.reduce = reduce
        self.by = by
        self.ref_key = ref_key
        self.origin = origin

    def _compute(self) -> OUTPUT_AttributeFunction:
        dbf: DBF = self._resolve_input(self.input_function)
        reduce_rf: RF = dbf[self.reduce]
        by_rf: RF = dbf[self.by]

        direction: RefDirection = _find_ref_direction(reduce_rf, by_rf, self.ref_key)

        reduced_rf: RF = RF(frozen=False)

        if direction is RefDirection.REDUCE:
            # reduce is source: reduce_tuple[ref_key] -> by (target)
            # keep reduce tuples whose ref_key points to a TF still in by_rf
            by_uuids: set[int] = {item.value.uuid for item in by_rf}
            for item in reduce_rf:
                if item.value[self.ref_key].uuid in by_uuids:
                    reduced_rf[item.key] = item.value
        elif direction is RefDirection.BY:
            # by is source: by_tuple[ref_key] -> reduce (target)
            # keep reduce tuples that are referenced by at least one by-tuple
            referenced_uuids: set[int] = set()
            for item in by_rf:
                referenced_uuids.add(item.value[self.ref_key].uuid)
            for item in reduce_rf:
                if item.value.uuid in referenced_uuids:
                    reduced_rf[item.key] = item.value
        else:
            raise ValueError(f"Unknown direction '{direction}'")
        # copy constraints from original RF so chained semijoins can detect direction
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
