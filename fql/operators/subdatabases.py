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

from fdm.attribute_functions import DBF
from fql.operators.APIs import Operator, OperatorInput
from fql.operators.semijoins import semijoin
from fql.plan.join_graph import JoinGraph, SemijoinStep

# ---------------------------------------------------------------------------
# Join graph types
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Operator
# ---------------------------------------------------------------------------


class subdatabase[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Compute the subdatabase of a DBF using Yannakakis semi-join reduction.

    The join graph is extracted automatically from ForeignValueConstraint metadata
    set up via .references() on the input RFs. Only **acyclic** join graphs are supported.
    Internally, the algorithm is expressed as a cascade of semijoin operators. This allows you
    to extract the logical plan and directly send it to a different backend.

    The constraints of the input DBF are preserved.

    The result is a DBF with the same relation names, but each RF reduced to only
    those tuples that participate in the full join.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        root: str | None = None,
    ):
        """Initialize the subdatabase operator.
        @param input_function: A DBF containing the relations to reduce. The input is
            resolved eagerly so that the join graph can be extracted and the semijoin
            pipeline can be assembled (required for plan extraction via to_plan()).
        @param root: Optional root node for the join tree. If None, auto-selected as
            the relation with no incoming references.
        """
        self.root = root

        # resolve input eagerly to extract the join graph and build the pipeline
        dbf: DBF = self._resolve_input(input_function)
        graph: JoinGraph = JoinGraph.from_dbf(dbf)

        if not graph.edges:
            # no references found — pipeline is just the input DBF
            self.input_function = dbf
            return

        selected_root: str = graph.select_root(self.root)
        steps: list[SemijoinStep] = graph.build_semijoin_cascade(selected_root)

        # assemble semijoin pipeline: each semijoin takes the previous as input
        pipeline: DBF | semijoin = dbf
        for step in steps:
            pipeline = semijoin[DBF, DBF](
                pipeline,
                reduce=step.reduce,
                by=step.by,
                ref_key=step.ref_key,
                origin="subdatabase",
            )

        # the last semijoin in the pipeline becomes our input_function
        self.input_function = pipeline

    def _compute(self) -> OUTPUT_AttributeFunction:
        return self._resolve_input(self.input_function)
