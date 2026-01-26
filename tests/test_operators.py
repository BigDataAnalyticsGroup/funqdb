import pytest

from fql.functions import TF, RF, DBF
from fql.operators.filters import filter_items
from fql.operators.joins import join, equi_join
from fql.operators.subdatabases import subdatabase
from fql.operators.transforms import (
    map_instance,
    transform_items,
    partition,
    group_by_aggregate,
)

from fql.util import Item, ReadOnlyError
from fql.operators.APIs import Operator

from tests.lib import _create_testdata, _users_customers_DBF, _subset_DBF


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


# filter the values in the users relation to only keep those in the "Dev" department
def filter_predicate(item: Item) -> bool:
    user: TF = item.value
    return user.department.name == "Dev"


def test_filter_items():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=filter_predicate,
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_complement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate_complement(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=filter_predicate_complement,
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 1
    for item in users_filtered:
        assert item.value.department.name == "Consulting"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"John"}


def test_DB_filter_keys():
    # get subdatabase:
    db_filtered: DBF = _subset_DBF({"users", "departments"}, frozen=True)

    assert type(db_filtered) == DBF
    assert len(db_filtered) == 2  # users and departments relations only

    assert type(db_filtered.users) == RF
    assert type(db_filtered.departments) == RF


def test_subdatabase_two_RFs():
    # note the idea to subdatabase along fk-relationships does not make much sense here as we do not have relational
    # data islands as in the relational model. In FDM, everything is connected via references. So, no need to
    # reconstruct those references: we can simply look them up.
    # So, what may make sense is to subdatabase users based on some attribute values, e.g., department name.

    # get subdatabase as input for the subdatabase operator, i.e. select a dbf having only the two relations we want
    # to work with:
    db_filtered: DBF = _users_customers_DBF(frozen=True)

    # join predicate to match users and customers by name:
    # def join_predicate(item_left: Item, item_right: Item) -> bool:
    #    return item_left.value.name == item_right.value.name

    # longer typed version:
    # reduced_DB: Operator[DBF, DBF] = subdatabase[DBF, DBF](
    #    join_predicate=join_predicate,
    #    left="users",
    #    right="customers",
    # )(db_filtered)

    # same thing again, but the short way of writing without type hints:
    # reduced_DBF: DBF = subdatabase[DBF, DBF](
    #    join_predicate, "users", "customers"
    # )(db_filtered)

    # same thing again, but with lambda join predicate:
    reduced_DBF: DBF = subdatabase[DBF, DBF](
        lambda item_left, item_right: item_left.value.name == item_right.value.name,
        "users",
        "customers",
    )(db_filtered)

    assert len(reduced_DBF.users) == 2  # Horst and John only
    users_names: set[str] = {user.value.name for user in reduced_DBF.users}
    assert users_names == {"Tom", "John"}
    assert len(reduced_DBF.customers) == 3  # Tom (2x) and John only
    customers_names: set[str] = {
        customer.value.name for customer in reduced_DBF.customers
    }
    assert customers_names == {"Tom", "John"}


def test_subdatabase_two_RFs_with_join_index():

    reduced_DBF: DBF = subdatabase[DBF, DBF](
        lambda item_left, item_right: item_left.value.name == item_right.value.name,
        "users",
        "customers",
        create_join_index=True,
    )(_users_customers_DBF())
    join_index: RF = reduced_DBF.join_index
    assert join_index is not None
    assert type(join_index) == RF
    assert len(join_index) == 3  # three matching pairs in the join index

    # check join index content:
    # no false positives, all returned pairs must match on user.name == customer.name:
    users_keys = {item.key for item in reduced_DBF.users}
    customers_keys = {item.key for item in reduced_DBF.customers}

    # create the cross product of all user keys and customer keys:
    cross_product_keys = {
        (u_key, c_key) for u_key in users_keys for c_key in customers_keys
    }

    # loop over join index and validate each pair, removing it from the cross product set:
    for item in join_index:
        left_key = item.value.left_key
        right_key = item.value.right_key
        user_name = reduced_DBF.users[left_key].name
        customer_name = reduced_DBF.customers[right_key].name
        assert user_name == customer_name
        # remove this validated pair from the cross product set:
        cross_product_keys.remove((left_key, right_key))

    # post condition: pairs in the cross product set should all be false positives now, i.e.,
    # cross_product_keys contains the complement of the join index
    for left_key, right_key in cross_product_keys:
        user_name = reduced_DBF.users[left_key].name
        customer_name = reduced_DBF.customers[right_key].name
        assert user_name != customer_name


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
        "users.value.name",
        "customers.value.name",
    )(_users_customers_DBF())
    # assert type(joined) == RF
    # assert len(joined) == 3  # three matching pairs in the join result
    # print()
    # for res in joined:
    # print(res.key)
    # res.value.print(flat=True)
    #    print(res.value)


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
