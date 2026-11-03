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

"""Every name a rendered statement may reference: it runs without other builtins.

FQL is trusted code. Withholding the builtins is no security boundary: an FQL
object reaches `__import__` through its `__globals__`. Put strings and names into
FQL as `repr(...)`, never by string formatting; write dates and times as
`date(...)`, `datetime(...)` and `timedelta(...)`. `check_database_name` guards
the name that FQL reads the database under.
"""

import keyword
import re
import unicodedata
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import fmean
from types import SimpleNamespace

from dateutil.relativedelta import relativedelta

from fdm.attribute_functions import DBF, RF, TF
from fql.operators.aggregates import (
    AggregationFunction,
    Avg,
    Count,
    Max,
    Median,
    Min,
    Sum,
    aggregate,
    𝜞,
)
from fql.operators.constraints import drop_reference
from fql.operators.filters import filter_values
from fql.operators.joins import join
from fql.operators.partition_and_aggregate import group_by_aggregate
from fql.operators.projections import project
from fql.operators.rank import rank_by
from fql.operators.subsets import subset
from fql.operators.transforms import key_to_value
from fql.predicates import (
    And,
    Eq,
    Exists,
    Gt,
    Gte,
    In,
    Like,
    Lt,
    Lte,
    Missing,
    Not,
    Or,
    Ref,
)

FQL_RUNTIME: dict[str, object] = {
    aggregate.__name__: aggregate,
    # Python reads 𝜞 as Γ (NFKC), so the key is "Γ"
    𝜞.__name__: 𝜞,
    group_by_aggregate.__name__: group_by_aggregate,
    key_to_value.__name__: key_to_value,
    filter_values.__name__: filter_values,
    join.__name__: join,
    drop_reference.__name__: drop_reference,
    project.__name__: project,
    rank_by.__name__: rank_by,
    subset.__name__: subset,
    Min.__name__: Min,
    Max.__name__: Max,
    Count.__name__: Count,
    Sum.__name__: Sum,
    Avg.__name__: Avg,
    Median.__name__: Median,
    AggregationFunction.__name__: AggregationFunction,
    DBF.__name__: DBF,
    RF.__name__: RF,
    TF.__name__: TF,
    # not the module: its public attributes would hand FQL `re.enum.sys`; `__globals__`
    # still reaches it, see the module docstring. SQL LIKE matches the whole value, and
    # its `%` and `_` also match a line break: render the pattern with `(?s)` (and with
    # `(?i)` for ILIKE).
    re.__name__: SimpleNamespace(fullmatch=re.fullmatch),
    float.__name__: float,
    Decimal.__name__: Decimal,
    list.__name__: list,
    set.__name__: set,
    len.__name__: len,
    sum.__name__: sum,
    min.__name__: min,
    max.__name__: max,
    fmean.__name__: fmean,
    date.__name__: date,
    datetime.__name__: datetime,
    timedelta.__name__: timedelta,
    relativedelta.__name__: relativedelta,
    Eq.__name__: Eq,
    Gt.__name__: Gt,
    Lt.__name__: Lt,
    Gte.__name__: Gte,
    Lte.__name__: Lte,
    Like.__name__: Like,
    In.__name__: In,
    Exists.__name__: Exists,
    Missing.__name__: Missing,
    And.__name__: And,
    Or.__name__: Or,
    Not.__name__: Not,
    Ref.__name__: Ref,
}


def check_database_name(name: str) -> None:
    """Raise `ValueError` when an FQL string cannot read the database under `name`:
    it is no Python name, a dunder name, or the runtime binds it."""
    if not is_python_name(name):
        raise ValueError(
            f"{name!r} cannot name the database: rendered statements read it as a "
            "plain Python name."
        )
    # Python gives these names a meaning: `__builtins__` replaces the builtins,
    # `__debug__` reads as a constant
    if name.startswith("__") and name.endswith("__"):
        raise ValueError(f"{name!r} cannot name the database: it is a dunder name.")
    if name in FQL_RUNTIME:
        raise ValueError(
            f"{name!r} cannot name the database: rendered statements reference it "
            "as part of the FQL runtime."
        )


def is_python_name(name: str) -> bool:
    """Whether Python reads `name` as itself: Python NFKC-normalizes identifiers."""
    normalized = unicodedata.normalize("NFKC", name)
    return name.isidentifier() and not keyword.iskeyword(name) and normalized == name
