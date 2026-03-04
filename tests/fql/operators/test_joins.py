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

from fdm.attribute_functions import RF, DBF
from fql.operators.joins import join, equi_join
from tests.lib import _users_customers_DBF


def test_flattening_join_two_RFs():
    joined: RF = join[DBF, RF](
        lambda item_left, item_right: item_left.value.name == item_right.value.name,
        "users",
        "customers",
    )(_users_customers_DBF())
    assert type(joined) == RF
    assert len(joined) == 3  # three matching pairs in the join result
    # print()
    # for res in joined:
    # print(res.key)
    # res.value.print(flat=True)
    #    print(res.value)


def test_flattening_equi_join_two_RFs():
    joined: RF = equi_join[DBF, RF](
        "name",
        "name",
        "users",
        "customers",
    )(_users_customers_DBF())
    # assert type(joined) == RF
    # assert len(joined) == 3  # three matching pairs in the join result
    # print()
    # for res in joined:
    # print(res.key)
    # res.value.print(flat=True)
    #    print(res.value)
    # assert False
