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

import difflib
from dataclasses import dataclass
from pathlib import Path

from benchmarks.tools.shared.code_language import CodeLanguage
from benchmarks.tools.shared.stage_column import StageColumn


@dataclass(frozen=True)
class CodeBlockDefinition:
    """The header of a code block and the language its code is shown in."""

    header: str
    language: CodeLanguage

    def get_view(self, code: str) -> "CodeBlockView":
        """The view of `code` in this block, pretty-printed in its language."""
        code_pretty = self.language.pretty(code)
        return CodeBlockView(
            header=self.header,
            language=self.language.value,
            code=code,
            code_pretty=code_pretty,
        )


@dataclass(frozen=True)
class ReportDefinition:
    """What a report shows: its status columns and code blocks, in display order."""

    name: str
    stages: list[StageColumn]
    input: CodeBlockDefinition
    output: CodeBlockDefinition
    intermediates: dict[str, CodeBlockDefinition]

    def __post_init__(self) -> None:
        self.validate()

    def create_empty_case(self, case_id: str) -> "ReportCase":
        """A case of this definition with empty code, no stage measured and nothing else set."""
        return ReportCase(
            definition=self,
            id=case_id,
            statuses={stage.key: None for stage in self.stages},
            input="",
            intermediates={key: "" for key in self.intermediates},
            output="",
            expected_output=None,
            sql_result=None,
            fql_result=None,
            error=None,
            limitation=None,
            settings={},
        )

    def validate(self) -> None:
        """Raise ValueError for a name that is not a plain file name or a repeated stage key."""
        # Runs again in `write_report`, because the collections stay mutable.
        if self.name in ("", ".", "..") or Path(self.name).name != self.name:
            raise ValueError(f"report name {self.name!r} is not a plain file name")

        keys = [stage.key for stage in self.stages]
        if len(set(keys)) != len(keys):
            raise ValueError(f"stage keys {keys} repeat")


@dataclass(frozen=True)
class ReportLimitation:
    """A known gap of the pipeline that a case documents."""

    # The identifier of the gap, for example "DUPLICATE_ELIMINATION".
    name: str
    # The group of the gap, for example "fql". The table row shows it as a badge.
    category: str
    description: str


@dataclass
class ReportCase:
    """The outcome of one case. The code is raw. ``None`` means that a value is absent."""

    definition: ReportDefinition
    id: str
    # The status of each stage, by stage key. ``None`` means that the stage was not measured.
    statuses: dict[str, bool | None]
    input: str
    # The code of each intermediate, by the key of its definition.
    intermediates: dict[str, str]
    output: str
    expected_output: str | None
    sql_result: str | None
    fql_result: str | None
    error: str | None
    limitation: ReportLimitation | None
    # Only the settings that are different from the default of the producer.
    settings: dict[str, str]

    def __post_init__(self) -> None:
        self.validate()

    @property
    def passed(self) -> bool:
        """True when a stage was measured, all measured stages passed and no error is set."""
        measured = [status for status in self.statuses.values() if status is not None]
        return bool(measured) and all(measured) and self.error is None

    def validate(self) -> None:
        """Raise ValueError when the statuses or intermediates do not match the definition."""
        # Runs again in `get_view` and `write_report`: the collections stay mutable.
        status_keys = set(self.statuses)
        stage_keys = {stage.key for stage in self.definition.stages}
        if status_keys != stage_keys:
            raise ValueError(
                f"{self.id}: statuses {sorted(status_keys)} do not match the "
                f"stages {sorted(stage_keys)}"
            )

        intermediate_keys = set(self.intermediates)
        defined_keys = set(self.definition.intermediates)
        if intermediate_keys != defined_keys:
            raise ValueError(
                f"{self.id}: intermediates {sorted(intermediate_keys)} do not match "
                f"the definition's intermediates {sorted(defined_keys)}"
            )

    def get_view(self) -> "ReportCaseView":
        """The view of the case: pretty code, intermediates in definition order."""
        self.validate()

        input_view = self.definition.input.get_view(self.input)
        intermediate_views = [
            block.get_view(self.intermediates[key])
            for key, block in self.definition.intermediates.items()
        ]
        output_view = self.definition.output.get_view(self.output)
        output_diff = self._output_diff(output_view.code_pretty)

        return ReportCaseView(
            id=self.id,
            passed=self.passed,
            statuses=dict(self.statuses),
            input=input_view,
            intermediates=intermediate_views,
            output=output_view,
            output_diff=output_diff,
            sql_result=self.sql_result,
            fql_result=self.fql_result,
            error=self.error,
            limitation=self.limitation,
            settings=dict(self.settings),
        )

    def _output_diff(self, output_pretty: str) -> list[str]:
        if self.expected_output is None:
            return []

        expected_pretty = self.definition.output.language.pretty(self.expected_output)
        expected_lines = expected_pretty.splitlines()
        output_lines = output_pretty.splitlines()
        # Compare the lines, not the strings. A trailing line break gives no diff line.
        if expected_lines == output_lines:
            return []

        diff = difflib.ndiff(expected_lines, output_lines)
        return [line for line in diff if not line.startswith("?")]


@dataclass(frozen=True)
class CodeBlockView:
    """A code block as the page shows it."""

    header: str
    # The tag of the page's syntax highlighter.
    language: str
    code: str
    code_pretty: str


@dataclass(frozen=True)
class ReportCaseView:
    """A case as the page shows it."""

    id: str
    passed: bool
    statuses: dict[str, bool | None]
    input: CodeBlockView
    # A list, not a dict: a JS object literal reorders integer-like keys.
    intermediates: list[CodeBlockView]
    output: CodeBlockView
    # The expected against the actual output as `difflib.ndiff` lines without the
    # intraline hints. Empty when nothing is expected or the lines agree.
    output_diff: list[str]
    sql_result: str | None
    fql_result: str | None
    error: str | None
    limitation: ReportLimitation | None
    settings: dict[str, str]
