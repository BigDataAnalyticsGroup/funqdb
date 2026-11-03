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

"""The names an FQL string can use, and the name it reads the database under."""

from datetime import date, datetime
from decimal import Decimal
from types import ModuleType

import pytest

from benchmarks.datasets import FQL_RUNTIME, check_database_name, is_python_name

PYTHON_NAMES = {"re", "float", "Decimal", "list", "set", "len", "sum"}  # rendered
PYTHON_NAMES |= {"min", "max", "fmean"}  # rendered
PYTHON_NAMES |= {"date", "datetime", "timedelta", "relativedelta"}  # rendered


def test_FQL_RUNTIME_binds_only_plain_non_dunder_names():
    """A rendered statement reads each object under a plain, non-dunder name."""
    for name in FQL_RUNTIME:  # every runtime binding
        assert is_python_name(name), name  # a statement can write the name
        assert not name.startswith("__"), name  # no `__import__`, `__builtins__`


def test_FQL_RUNTIME_binds_exactly_the_names_of_the_spec():
    """A name added by accident, or removed, fails here; `SPEC.md` lists the groups."""
    operators = {"aggregate", "Γ", "group_by_aggregate", "key_to_value"}  # reshape
    operators |= {"filter_values", "join", "drop_reference", "project"}  # select
    operators |= {"rank_by", "subset"}  # the remaining operators
    aggregates = {"Min", "Max", "Count", "Sum", "Avg", "Median"}  # by name
    aggregates |= {"AggregationFunction"}  # the base type of a custom aggregate
    functions = {"DBF", "RF", "TF"}  # the FDM types
    predicates = {"Eq", "Gt", "Lt", "Gte", "Lte", "Like", "In"}  # comparisons
    predicates |= {"Exists", "Missing", "And", "Or", "Not", "Ref"}  # the structure

    expected = operators | aggregates | functions | PYTHON_NAMES | predicates  # all

    assert set(FQL_RUNTIME) == expected  # no more and no fewer
    assert list(vars(FQL_RUNTIME["re"])) == ["fullmatch"]  # `re` offers nothing else


def test_FQL_RUNTIME_binds_each_value_under_its_own_name():
    """A slip such as `Gte.__name__: Gt` keeps the names but binds the wrong object."""
    named = {  # `re` is a namespace without a `__name__`
        name: value for name, value in FQL_RUNTIME.items() if name != "re"
    }

    for name, value in named.items():  # every other binding
        assert value.__name__ == name  # the object is the one that the name says


def test_FQL_RUNTIME_binds_FQL_and_FDM_names_from_funqdb():
    """A namesake such as `sqlglot.expressions.Count` has the name, not the module."""
    funqdb_names = set(FQL_RUNTIME) - PYTHON_NAMES  # the operators, predicates, types

    for name in funqdb_names:  # every FQL and FDM binding
        package = FQL_RUNTIME[name].__module__.split(".")[0]  # e.g. `fql` of `fql.x`
        assert package in ("fql", "fdm"), name  # not from another package


def test_FQL_RUNTIME_public_attributes_hold_no_module():
    """A bound module would hand FQL the operating system without a dunder name."""
    for name, value in FQL_RUNTIME.items():  # every runtime binding
        assert not isinstance(value, ModuleType), name  # the value itself
        public = [attribute for attribute in dir(value) if attribute[0] != "_"]  # API
        for attribute in public:  # every name FQL can write after the dot
            bound = getattr(value, attribute)  # what FQL would get
            assert not isinstance(bound, ModuleType), f"{name}.{attribute}"  # refused


@pytest.mark.parametrize(
    "fql, expected",
    [
        ("date(2020, 1, 31) + relativedelta(months=1)", date(2020, 2, 29)),
        ("datetime(2020, 1, 1) + timedelta(days=1)", datetime(2020, 1, 2)),
        ("Decimal('0.1') + Decimal('0.2')", Decimal("0.3")),
        ("float('1.5')", 1.5),
        ("list(set([1, 1]))", [1]),
        ("len([1, 2])", 2),
        ("sum([1, 2])", 3),
        ("min([1, 2])", 1),
        ("max([1, 2])", 2),
        ("fmean([1, 2])", 1.5),
        ("re.fullmatch('a.', 'ab') is not None", True),  # the whole value matches
        ("re.fullmatch('a', 'ab') is None", True),  # `re.match` would match
        ("re.fullmatch('b', 'ab') is None", True),  # `re.search` would match
        (r"re.fullmatch('a', 'a\n') is None", True),  # `$` would match before `\n`
        (r"re.fullmatch('(?s)a.b', 'a\nb') is not None", True),  # `(?s)` flag
    ],
)
def test_FQL_RUNTIME_python_name_runs_in_execute_fql(dataset, fql, expected):
    """Each Python name of the runtime is what a rendered statement expects."""
    result = dataset.execute_fql(fql)  # runs without other builtins

    assert result.rows == ((expected,),)  # one scalar, the expected value


@pytest.mark.parametrize(
    "name, reason",
    [
        ("not a name", "plain Python name"),
        ("class", "plain Python name"),
        ("join", "FQL runtime"),
        ("ｄａｔｅ", "plain Python name"),
    ],
    ids=["no identifier", "keyword", "bound by the runtime", "read as date"],
)
def test_check_database_name_rejects_unusable_name(name, reason):
    """The error says why the name cannot hold the database."""
    with pytest.raises(ValueError, match=reason):  # the reason is named
        check_database_name(name)  # refused


@pytest.mark.parametrize("name", ["__builtins__", "__debug__"])
def test_check_database_name_rejects_dunder_name(name):
    """`__builtins__` would replace the builtins; `__debug__` reads as a constant."""
    with pytest.raises(ValueError, match="dunder name"):  # the reason is named
        check_database_name(name)  # a Python name the runtime lacks


def test_check_database_name_error_shows_hidden_characters():
    """A trailing line break is visible in the message, not only a broken line."""
    with pytest.raises(ValueError, match=r"'dbf\\n'"):  # the escaped form
        check_database_name("dbf\n")  # looks like `dbf` when printed


@pytest.mark.parametrize("name", ["dbf", "__dbf", "dbf__"])
def test_check_database_name_accepts_unbound_python_name(name):
    """A Python name that the runtime does not bind and no dunder can hold it."""
    check_database_name(name)  # no error: Python gives these names no meaning


def test_is_python_name_rejects_a_name_python_normalizes():
    """Python reads `𝜞` as `Γ`, so the name cannot address its own attribute."""
    normalizing = "𝜞"  # an identifier that NFKC changes

    assert is_python_name(normalizing) is False  # Python would read another name


def test_is_python_name_accepts_an_identifier_that_is_no_keyword():
    """A plain identifier reads as itself."""
    assert is_python_name("title") is True  # identifier, no keyword, NFKC-stable
