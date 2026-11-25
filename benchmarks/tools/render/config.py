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

"""The style options of rendered FQL, and `Config`, which selects one style per option."""

import string
from dataclasses import dataclass
from enum import Enum

from benchmarks.datasets import check_database_name, is_python_name
from benchmarks.tools.render.fql_names import (
    FDM_LOOKUP_SEPARATOR,
    ITEM_NAME,
    NESTED_NAMES,
    SHADOWED_NAMES,
    TUPLE_NAME,
    UNNESTED_NAMES,
    reject_unaddressable_name,
)
from fdm.API import AttributeFunction
from fql.operators.aggregates import 𝜞, aggregate


class AttributeAccess(Enum):
    """How a rendered statement reads an attribute off a value:

    DOT        tf.department.name
    SUBSCRIPT  tf['department']['name']
    PATH       tf['department__name']
    """

    DOT = "dot"
    SUBSCRIPT = "subscript"
    PATH = "path"

    def render(self, base: str, names: list[str]) -> str:
        """The attribute path `names`, read off the Python expression `base`."""
        for name in names:
            reject_unaddressable_name(name)

        if not names:
            return base

        if self is AttributeAccess.PATH:
            return f"{base}[{FDM_LOOKUP_SEPARATOR.join(names)!r}]"

        for name in names:
            base = (
                f"{base}.{name}"
                if self is AttributeAccess.DOT and _is_readable_as_attribute(name)
                else f"{base}[{name!r}]"
            )

        return base


class OperatorNaming(Enum):
    """What a rendered statement calls the operators that have two names:

    WORDS    .where(...)  .project(...)  .rename(...)  aggregate(...)
    SYMBOLS  .σ(...)      .π(...)        .ρ(...)       Γ(...)
    """

    WORDS = (
        AttributeFunction.where.__name__,
        AttributeFunction.project.__name__,
        AttributeFunction.rename.__name__,
        aggregate.__name__,
    )
    SYMBOLS = (
        AttributeFunction.𝛔.__name__,
        AttributeFunction.π.__name__,
        AttributeFunction.ρ.__name__,
        𝜞.__name__,
    )

    def __init__(
        self, filter_name: str, project_name: str, rename_name: str, aggregate_name: str
    ) -> None:
        self.filter_name = filter_name
        self.project_name = project_name
        self.rename_name = rename_name
        self.aggregate_name = aggregate_name


class CallChaining(Enum):
    """How a rendered statement applies the operators that have both forms:

    OPERATOR  project(filter_values(af, lambda tf: tf.x == 1).result, 'x').result
    METHOD    af.where(lambda i: i.value.x == 1).project('x')
    """

    OPERATOR = (TUPLE_NAME, "")
    METHOD = (ITEM_NAME, ".value")

    def __init__(self, binding: str, value_suffix: str) -> None:
        self.binding = binding
        self.value_suffix = value_suffix

    def render_value_access(self, parameter: str) -> str:
        """The tuple that the lambda `parameter` of a filter reads."""
        return f"{parameter}{self.value_suffix}"


class PredicateForm(Enum):
    """How a rendered statement phrases a filter's predicate:

    STRUCTURED  .where(Eq('name', 'Horst'))
    LAMBDA      .where(lambda i: i.value.name == 'Horst')
    """

    STRUCTURED = "structured"
    LAMBDA = "lambda"


class OperatorResult(Enum):
    """How a rendered statement takes an operator's result:

    PROPERTY  aggregate(...).result
    CALL      aggregate(...)()
    """

    PROPERTY = "property"
    CALL = "call"

    def render(self, operator: str) -> str:
        """The result of the rendered operator call `operator`."""
        if self is OperatorResult.PROPERTY:
            return f"{operator}.result"

        return f"{operator}()"


OPTIONS: dict[str, type[Enum]] = {
    "attribute_access": AttributeAccess,
    "operator_naming": OperatorNaming,
    "operator_result": OperatorResult,
    "call_chaining": CallChaining,
    "predicate_form": PredicateForm,
}


@dataclass(frozen=True)
class Config:
    """The database name that rendered FQL reads, and one style per option."""

    dbf_name: str = "dbf"
    attribute_access: AttributeAccess = AttributeAccess.DOT
    operator_naming: OperatorNaming = OperatorNaming.WORDS
    operator_result: OperatorResult = OperatorResult.PROPERTY
    call_chaining: CallChaining = CallChaining.OPERATOR
    predicate_form: PredicateForm = PredicateForm.STRUCTURED

    def __post_init__(self) -> None:
        """Rejects a database name that a rendered binding would hide."""
        check_database_name(self.dbf_name)
        # a nested name is rejected with any number suffix, also a zero-led one
        # such as i0 or tf01 that no nesting level produces
        unsuffixed = self.dbf_name.rstrip(string.digits)
        if self.dbf_name in UNNESTED_NAMES or unsuffixed in NESTED_NAMES:
            raise ValueError(
                f"{self.dbf_name!r} cannot name the database: rendered lambdas and "
                "loops bind it, which hides the database inside them."
            )

    @property
    def settings(self) -> dict[str, str]:
        """The selected style of each option, by name."""
        return {option: getattr(self, option).name for option in OPTIONS}

    @classmethod
    def from_settings(cls, settings: dict[str, str]) -> "Config":
        """The config with the style `settings` names per option; raises `KeyError`
        for an option or a style that does not exist."""
        chosen = {option: OPTIONS[option][style] for option, style in settings.items()}
        return cls(**chosen)

    @staticmethod
    def possible_styles() -> dict[str, list[str]]:
        """The names of the styles of each option, by option."""
        return {
            option: [style.name for style in styles]
            for option, styles in OPTIONS.items()
        }


def _is_readable_as_attribute(name: str) -> bool:
    """Whether `base.name` reaches the value stored under `name`: Python has to be
    able to read it, and no `AttributeFunction` member may answer first."""
    return is_python_name(name) and name not in SHADOWED_NAMES
