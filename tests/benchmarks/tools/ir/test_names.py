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

"""Names that the transpiler makes up."""

import pytest

from benchmarks.datasets import is_python_name
from benchmarks.tools.ir.names import as_python_name, fresh_name


def test_fresh_name_takes_suffix_zero_when_it_is_free():
    """Nothing taken: the first suffix is `_0`."""
    assert fresh_name("title", set()) == "title_0"  # the smallest suffix


def test_fresh_name_skips_every_taken_suffix():
    """Taken suffixes are skipped up to the first free one."""
    taken = {"title_0", "title_1", "title_3"}  # a gap at 2
    assert fresh_name("title", taken) == "title_2"  # the gap is the smallest free


@pytest.mark.parametrize(
    ("text", "name"),
    [
        ("title", "title"),  # already a Python name: unchanged
        ("order details", "order_details"),  # a space is no name character
        ("a-b.c", "a_b_c"),  # nor are a hyphen and a dot
        ("1st", "_1st"),  # a name cannot start with a digit
        ("ｔitle", "title"),  # Python reads the full-width t as a plain t
        ("class", "_class"),  # a keyword is no name
        ("None", "_None"),  # nor is a constant keyword
    ],
)
def test_as_python_name_replaces_what_python_does_not_read_as_a_name(text, name):
    """Each result is a name that Python reads as itself."""
    python_name = as_python_name(text)  # the name made from text

    assert python_name == name  # the expected spelling
    assert is_python_name(python_name)  # and Python reads it as itself
