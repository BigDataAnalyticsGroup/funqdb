import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.APIs import Operator
from fql.operators.transforms import (
    map_instance,
    transform_items,
    partition,
    group_by_aggregate,
)
from fql.util import Item, ReadOnlyError
from tests.lib import _create_testdata


def test_map_instance():
    """map input RF to output RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    map_RF: Operator[RF, RF] = map_instance[RF, RF](mapping_function=lambda el: el)
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
        users_transformed: RF = transform_RF(users)

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    transform_RF = transform_items[RF, RF](
        transformation_function=transformation_function_non_modifying,
    )
    with pytest.raises(ReadOnlyError):
        users_transformed: RF = transform_RF(users)


def test_TransformValues_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    tramsform_RF: Operator[RF, RF] = transform_items[RF, RF](
        transformation_function=transformation_function_non_modifying,
        output_factory=lambda _: RF(),
    )
    users_transformed: RF = tramsform_RF(users)
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


def test_group_by_aggregate_stepwise():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    partitions = partition(lambda i: "Tom" if i.value.name == "Tom" else "not Tom")(
        customers
    )

    # take the DBF of RF with partitions and return one RF with one aggregated TFs per partition:
    aggregates = transform_items[DBF, RF](
        transformation_function=lambda item: Item(
            key=item.key, value=TF({"count": len(item.value)})
        ),
        output_factory=lambda _: RF(),
    )(partitions)

    assert len(aggregates) == 2
    assert type(aggregates) == RF

    tom_aggregate: TF = aggregates["Tom"]
    assert type(tom_aggregate) == TF
    assert tom_aggregate.count == 2

    not_tom_aggregate: TF = aggregates["not Tom"]
    assert type(not_tom_aggregate) == TF
    assert not_tom_aggregate.count == 3


def test_group_by_aggregate_single_operator():
    aggregates: RF = group_by_aggregate(
        grouping_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom",
        aggregation_function=lambda item: Item(
            key=item.key, value=TF({"count": len(item.value)})
        ),
    )(_create_testdata(frozen=True).customers)

    assert len(aggregates) == 2
    assert type(aggregates) == RF

    tom_aggregate: TF = aggregates["Tom"]
    assert type(tom_aggregate) == TF
    assert tom_aggregate.count == 2

    not_tom_aggregate: TF = aggregates["not Tom"]
    assert type(not_tom_aggregate) == TF
    assert not_tom_aggregate.count == 3
