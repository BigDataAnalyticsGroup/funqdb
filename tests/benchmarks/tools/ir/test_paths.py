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

"""How a path names the attributes that lead to the tuple of its alias."""

from benchmarks.tools.ir.paths import Path

# t is reached from the root mc along its reference `movie`
TITLE_FROM_MC = Path(0, "t", "mc", ("movie",))


def test_path_segments_of_a_root_tuple_are_its_ref_keys():
    """A root tuple holds the references itself, so the path starts with them."""
    assert TITLE_FROM_MC.segments(joined=False) == ["movie"]  # mc.movie


def test_path_segments_of_a_join_tuple_start_with_the_root():
    """A join tuple holds each root under its alias, so the path starts there."""
    assert TITLE_FROM_MC.segments(joined=True) == ["mc", "movie"]  # join.mc.movie


def test_path_segments_of_the_root_itself_are_empty_off_its_own_tuple():
    """The root reaches itself without a reference."""
    root = Path(0, "mc", "mc", ())  # mc from mc

    assert root.segments(joined=False) == []  # the root tuple itself
    assert root.segments(joined=True) == ["mc"]  # the root inside the join tuple
