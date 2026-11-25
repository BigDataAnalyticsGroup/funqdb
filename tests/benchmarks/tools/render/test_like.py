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

"""The shapes of SQL LIKE patterns and their regular expressions."""

import re

from benchmarks.tools.render.like import LikePattern, LikeShape


def test_LikePattern_shape_without_wildcards_is_exact():
    """A pattern without `%` or `_` matches only itself."""
    # the optimizer folds a pattern without wildcards into `=`, so no SQL case
    # reaches the exact shape
    assert LikePattern("Heat").shape is LikeShape.EXACT  # no wildcard


def test_LikePattern_shape_with_a_percent_inside_needs_a_regex():
    """Only `%` at the start and the end is a substring test; one inside is not."""
    assert LikePattern("%a%").shape is LikeShape.CONTAINS  # a substring test
    assert LikePattern("%a%b%").shape is LikeShape.REGEX  # `a`, then `b`, in order


def test_LikePattern_regex_percent_spans_a_line_break():
    """SQL's `%` matches any characters, also a line break."""
    regex = LikePattern("a%b").regex  # `a`, anything, `b`

    assert re.fullmatch(regex, "a\nb")  # the line break is part of `%`


def test_LikePattern_regex_underscore_matches_exactly_one_character():
    """SQL's `_` matches one character: not none, and not two."""
    regex = LikePattern("a_").regex  # `a` and one more character

    assert re.fullmatch(regex, "ab")  # exactly two characters
    assert not re.fullmatch(regex, "a")  # `_` needs a character
    assert not re.fullmatch(regex, "ab\n")  # a third character, the line break
