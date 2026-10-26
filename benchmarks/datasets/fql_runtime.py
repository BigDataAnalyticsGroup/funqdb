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

"""Every name a rendered statement may reference: it runs without builtins.

FQL is trusted code. Withholding the builtins is no security boundary: an FQL
object reaches `__import__` through its `__globals__`. Put an SQL literal into FQL
as `repr(value)`, never by string formatting.
"""

from fdm.attribute_functions import TF
from fql.predicates import Exists, Missing

FQL_RUNTIME: dict[str, object] = {
    TF.__name__: TF,
    set.__name__: set,
    len.__name__: len,
    Exists.__name__: Exists,
    Missing.__name__: Missing,
}
