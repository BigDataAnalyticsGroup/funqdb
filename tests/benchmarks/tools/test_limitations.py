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

"""The limitation catalog, and the error the transpiler raises for an entry."""

import re
from pathlib import Path

import pytest

from benchmarks.tools.limitations import (
    Limitation,
    LimitationCategory,
    UnsupportedSql,
)

# the index page that lists every limitation under its category
INDEX = Path(__file__).parents[3] / "benchmarks" / "tools" / "Limitations.md"


def test_limitation_description_is_not_blank_for_any_entry():
    """A blank description would say nothing about its entry."""
    blank = [lim.name for lim in Limitation if not lim.description.strip()]  # offenders

    assert blank == []  # every entry says what it is


def test_limitation_has_no_alias():
    """Two entries with an equal value would merge into one member under two names;
    `@unique` must refuse that when the module loads."""
    assert len(Limitation.__members__) == len(Limitation)  # every name its own member


def test_limitation_description_is_unique_per_entry():
    """Two entries of different categories must not say the same either."""
    descriptions = [limitation.description for limitation in Limitation]  # all

    assert len(descriptions) == len(set(descriptions))  # no two alike


def test_unsupported_sql_str_names_its_category_and_limitation_first():
    """The message leads with the category and the entry's name, then the detail
    of the one query at hand."""
    limitation = Limitation.LIMIT_WITHOUT_ORDER  # a rejected entry
    error = UnsupportedSql(limitation, "LIMIT 3 has no ORDER BY.")  # site detail

    expected = "[rejected] LIMIT_WITHOUT_ORDER: LIMIT 3 has no ORDER BY."  # format
    assert str(error) == expected  # category, name, then the detail
    assert error.limitation is limitation  # the entry stays inspectable


def test_limitations_index_has_one_section_per_category():
    """A limitation under any other level-2 heading would escape the next test."""
    text = INDEX.read_text(encoding="utf-8")  # the index page
    headings = re.findall(r"^## (.+)$", text, flags=re.MULTILINE)  # in file order

    assert headings == list(LimitationCategory)  # the categories, in enum order


def test_limitations_index_names_no_removed_limitation():
    """Every upper-case name in backticks is a limitation, in a table or a list alike."""
    text = INDEX.read_text(encoding="utf-8")  # the index page
    found = re.findall(r"`([A-Z][A-Z_]*_[A-Z_]+)`", text)  # NAME_LIKE_THIS
    runtime_names = {"FQL_RUNTIME"}  # the one such name that is no limitation

    assert set(found) - runtime_names <= set(Limitation.__members__)  # none removed


@pytest.mark.parametrize("category", list(LimitationCategory), ids=str)
def test_limitations_index_lists_each_limitation_under_its_category(
    category: LimitationCategory,
):
    """Limitations.md has one section per category, and it lists exactly the
    limitations of that category, each once, so the index cannot drift from the
    catalog."""
    text = INDEX.read_text(encoding="utf-8")  # the index page
    sections = re.split(r"^## ", text, flags=re.MULTILINE)  # split at categories
    heading = f"{category}\n"  # the category's own section heading
    section = next(s for s in sections if s.startswith(heading))  # this one
    listed = re.findall(r"^\| `(\w+)`\s+\|", section, flags=re.MULTILINE)  # names

    expected = [lim.name for lim in Limitation if lim.category is category]  # enum
    assert sorted(listed) == sorted(expected)  # same entries, none twice
