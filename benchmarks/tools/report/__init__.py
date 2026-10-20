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

"""Write a report page of pipeline runs: build a `ReportDefinition`, fill one
`ReportCase` per run from `create_empty_case`, then call `write_report`."""

from benchmarks.tools.report.contracts import (
    CodeBlockDefinition,
    ReportCase,
    ReportDefinition,
    ReportLimitation,
)
from benchmarks.tools.report.writer import write_report
from benchmarks.tools.shared.code_language import CodeLanguage
from benchmarks.tools.shared.stage_column import StageColumn

__all__ = [
    "CodeBlockDefinition",
    "CodeLanguage",
    "ReportCase",
    "ReportDefinition",
    "ReportLimitation",
    "StageColumn",
    "write_report",
]
