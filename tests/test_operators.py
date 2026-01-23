import pytest

from fql.functions import TF, RF, DBF
from fql.util import Item, ReadOnlyError
from fql.operators import Operator, MapInstance, TransformValues, FilterValues
from tests.lib import _create_testdata


def test_MapInstance():
    """map input RF to output RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    map_instance: Operator[RF, RF] = MapInstance[RF, RF](mapping_function=lambda el: el)
    users_mapped: RF = map_instance(users)
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
    transformed_values: Operator[RF, RF] = TransformValues[RF, RF](
        transformation_function=transformation_function_modifying
    )

    # must fail as the input RF is frozen and the transformation_function tries to modify it:
    with pytest.raises(ReadOnlyError):
        users_transformed: RF = transformed_values(users)

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    transformed_values = TransformValues[RF, RF](
        transformation_function=transformation_function_non_modifying
    )
    with pytest.raises(ReadOnlyError):
        users_transformed: RF = transformed_values(users)


def test_TransformValues_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    transformed_values: Operator[RF, RF] = TransformValues[RF, RF](
        transformation_function=transformation_function_non_modifying,
        output_factory=lambda: RF(),
    )
    users_transformed: RF = transformed_values(users)
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


def test_FilterValues():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filtered_values: Operator[RF, RF] = FilterValues[RF, RF](
        filter_predicate=filter_predicate,
        output_factory=lambda: RF(),
    )
    users_filtered: RF = filtered_values(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_FilterValuesComplement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    filtered_values: Operator[RF, RF] = FilterValues[RF, RF](
        filter_predicate=filter_predicate,
        output_factory=lambda: RF(),
    )
    users_filtered: RF = filtered_values(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 1
    for item in users_filtered:
        assert item.value.department.name == "Consulting"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"John"}
