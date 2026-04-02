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

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.transforms import (
    transform_items,
    transform,
)
from fql.util import Item, ReadOnlyError
from tests.lib import _create_testdata


def test_transform_instance():
    """map input RF to output RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    users_mapped: RF = transform[RF, RF](users, transformation_function=lambda el: el).result
    assert type(users_mapped) == RF
    assert users == users_mapped


def transformation_function_modifying(item: Item) -> Item | None:
    """an item transformation_function modifying the input and returning it"""
    user: TF = item.value
    user.name = user.name.upper()
    return item


def transformation_function_non_modifying(item: Item) -> Item | None:
    """an item transformation_function returning a modified copy of the input"""
    user: TF = item.value
    tf_new = TF()
    tf_new.name = user.name.upper()
    return Item(key=item.key, value=tf_new)


def test_transform_items():
    """map input RF to output RF using filter mapping function to return only some values in the input RF. Modifies the
    input RF in place. This should fail for frozen RFs."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # transform the values in the users relation (note: this will modify the original RF in the db)
    # must fail as the input RF is frozen and the transformation_function tries to modify it:
    with pytest.raises(ReadOnlyError):
        transform_items[RF, RF](
            users,
            transformation_function=transformation_function_modifying,
            output_factory=lambda _: RF(),
        ).result

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    with pytest.raises(ReadOnlyError):
        transform_items[RF, RF](
            users,
            transformation_function=transformation_function_non_modifying,
        ).result


def test_transform_items_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    users_transformed: RF = transform_items[RF, RF](
        users,
        transformation_function=transformation_function_non_modifying,
        output_factory=lambda _: RF(),
    ).result
    assert type(users_transformed) == RF

    users_names = {user.value.name for user in users}
    transformed_user_names = {user.value.name for user in users_transformed}
    # manual set comparison should fail now:
    assert users_names != transformed_user_names
    assert {name.upper() for name in users_names} == transformed_user_names
    assert users_names == {
        name[0] + name[1:].lower() for name in transformed_user_names
    }
