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

"""The names an FQL string can use."""

from benchmarks.datasets import FQL_RUNTIME


def test_FQL_RUNTIME_binds_only_plain_names_and_no_builtin_escape():
    """A rendered statement reads each object under a plain name; none leaks builtins."""
    for name in FQL_RUNTIME:  # every runtime binding
        assert name.isidentifier(), name  # a statement can write the name
        assert not name.startswith("__"), name  # no `__import__`, `__builtins__`
