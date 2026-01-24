import pytest

from fql.functions import TF, RF, DBF
from fql.util import Item, ReadOnlyError
from fql.operators import (
    Operator,
    map_instance,
    transform_values,
    filter_values,
    subdatabase,
)
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
    transform_RF: Operator[RF, RF] = transform_values[RF, RF](
        transformation_function=transformation_function_modifying,
        output_factory=lambda _: RF(),
    )

    # must fail as the input RF is frozen and the transformation_function tries to modify it:
    with pytest.raises(ReadOnlyError):
        users_transformed: RF = transform_RF(users)

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    transform_RF = transform_values[RF, RF](
        transformation_function=transformation_function_non_modifying,
    )
    with pytest.raises(ReadOnlyError):
        users_transformed: RF = transform_RF(users)


def test_TransformValues_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    tramsform_RF: Operator[RF, RF] = transform_values[RF, RF](
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


def test_filter_values():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_values[RF, RF](
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


def test_filter_values_complement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate_complement(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    filter_RF: Operator[RF, RF] = filter_values[RF, RF](
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
    db_filtered: DBF = filter_values(
        lambda i: i.key in ["users", "departments"], lambda _: DBF()
    )(_create_testdata(frozen=True))

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
    db_filtered: DBF = filter_values(
        lambda i: i.key in ["users", "customers"], lambda _: DBF()
    )(_create_testdata(frozen=True))

    # join predicate to match users and customers by name:
    # def join_predicate(item_left: Item, item_right: Item) -> bool:
    #    return item_left.value.name == item_right.value.name

    # longer typed version:
    # reduced_DB: Operator[DBF, DBF] = subdatabase[DBF, DBF](
    #    join_predicate=join_predicate,
    #    output_DBF_factory=lambda _: DBF(),
    #    output_RF_factory=lambda _: RF(),
    #    left="users",
    #    right="customers",
    # )(db_filtered)

    # same thing again, but the short way of writing without type hints:
    # reduced_DBF: DBF = subdatabase[DBF, DBF](
    #    join_predicate, lambda _: DBF(), lambda _: RF(), "users", "customers"
    # )(db_filtered)

    # same thing again, but with lambda join predicate:
    reduced_DBF: DBF = subdatabase[DBF, DBF](
        lambda item_left, item_right: item_left.value.name == item_right.value.name,
        lambda _: DBF(),
        lambda _: RF(),
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
