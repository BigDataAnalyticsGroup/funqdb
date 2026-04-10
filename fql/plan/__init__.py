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

"""The ``fql.plan`` package: logical IR and extractor for FQL operator pipelines.

The core idea: an FQL pipeline is already a tree of lazy ``Operator`` instances
(see ``fql/operators/APIs.py``). This package provides

1. a small, backend-agnostic logical IR (``ir.py``), and
2. a walker (``extract.py``) that turns an un-executed operator tree into such
   an IR without triggering any computation.

The IR can then be serialized (JSON) and shipped to a backend, or used as a
basis for partitioning (executable subtree vs. local residual) and later
optimization. See the design notes accompanying PR 1 for background.
"""

from fql.plan.ir import IR_VERSION, LeafRef, LogicalPlan, Opaque, PlanNode
from fql.plan.extract import extract, extract_plan

__all__ = [
    "IR_VERSION",
    "LeafRef",
    "LogicalPlan",
    "Opaque",
    "PlanNode",
    "extract",
    "extract_plan",
]
