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
from fql.operators.APIs import Operator
from fql.operators.transforms import (
    transform_items,
    partition,
    transform,
    group_by,
)
from fql.util import Item, ReadOnlyError
from tests.lib import _create_testdata


def test_transform_instance():
    """map input RF to output RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    map_RF: Operator[RF, RF] = transform[RF, RF](transformation_function=lambda el: el)
    users_mapped: RF = map_RF(users)
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


def test_TransformValues():
    """map input RF to output RF using filter mapping function to return only some values in the input RF. Modifies the
    input RF in place. This should fail for frozen RFs."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # transform the values in the users relation (note: this will modify the original RF in the db)
    transform_RF: Operator[RF, RF] = transform_items[RF, RF](
        transformation_function=transformation_function_modifying,
        output_factory=lambda _: RF(),
    )

    # must fail as the input RF is frozen and the transformation_function tries to modify it:
    with pytest.raises(ReadOnlyError):
        transform_RF(users)

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    transform_RF = transform_items[RF, RF](
        transformation_function=transformation_function_non_modifying,
    )
    with pytest.raises(ReadOnlyError):
        transform_RF(users)


def test_transform_items_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    transform_RF: Operator[RF, RF] = transform_items[RF, RF](
        transformation_function=transformation_function_non_modifying,
        output_factory=lambda _: RF(),
    )
    users_transformed: RF = transform_RF(users)
    assert type(users_transformed) == RF

    users_names = {user.value.name for user in users}
    transformed_user_names = {user.value.name for user in users_transformed}
    # manual set comparison should fail now:
    assert users_names != transformed_user_names
    assert {name.upper() for name in users_names} == transformed_user_names
    assert users_names == {
        name[0] + name[1:].lower() for name in transformed_user_names
    }


def test_partitioning():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    partitions = partition(lambda i: "Tom" if i.value.name == "Tom" else "not Tom")(
        customers
    )
    assert len(partitions) == 2
    assert type(partitions) == DBF

    tom_partition: RF = partitions["Tom"]
    assert type(tom_partition) == RF
    assert len(tom_partition) == 2
    for item in tom_partition:
        assert item.value.name == "Tom"

    not_tom_partition: RF = partitions["not Tom"]
    assert type(not_tom_partition) == RF
    assert len(not_tom_partition) == 3
    for item in not_tom_partition:
        assert item.value.name != "Tom"


def test_partitioning_and_group_by_composed_partitioning_key():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    for i in range(2):
        partitions: DBF | None = None
        if i == 0:
            # generic partitioning based on a partitioning function:
            partitions = partition(lambda i: (i.value.name, i.value.company))(customers)
        else:
            # explicit group by building partitions based on equality of multiple attributes:
            partitions = group_by("name", "company")(customers)
        assert len(partitions) == 4
        assert type(partitions) == DBF

        tom_whatever_partition: RF = partitions[("Tom", "whatever gmbh")]
        assert type(tom_whatever_partition) == RF
        assert len(tom_whatever_partition) == 2

        john_whatever_partition: RF = partitions[("John", "whatever gmbh")]
        assert type(john_whatever_partition) == RF
        assert len(john_whatever_partition) == 1

        peter_ppmi_partition: RF = partitions[("Peter", "Peter, Paul, and Mary Inc.")]
        assert type(peter_ppmi_partition) == RF
        assert len(peter_ppmi_partition) == 1

        frank_masterhorst_partition: RF = partitions[("Frank", "Masterhorst")]
        assert type(frank_masterhorst_partition) == RF
        assert len(frank_masterhorst_partition) == 1
