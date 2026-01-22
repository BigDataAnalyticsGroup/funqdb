import pytest

from fql.functions import TF, RF, DBF
from fql.APIs import Item
from fql.operators import Operator, MapInstance, TransformValues
from tests.lib import _create_testdata


def test_MapInstance():
    """map input RF to putput RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    map_instance: Operator[RF, RF] = MapInstance[RF, RF](mapping_function=lambda el: el)
    users_mapped: RF = map_instance(users)
    assert type(users_mapped) == RF
    assert users == users_mapped


# an item transformation_function that returns the item as is
def transformation_function(item: Item) -> Item | None:
    user: TF = item.value
    user.name = user.name.upper()
    return item


def test_TransformValues():
    """map input RF to output RF using filter mapping function to return only some values in the input RF."""
    db: DBF = _create_testdata()
    users: RF = db.users

    # transform the values in the users relation (note: this will modify the original RF in the db)
    transformed_values: Operator[RF, RF] = TransformValues[RF, RF](
        transformation_function=transformation_function
    )
    users_transformed: RF = transformed_values(users)
    assert type(users_transformed) == RF
    assert users == users_transformed

    # get test data again:
    db_old: DBF = _create_testdata()
    users_old: RF = db_old.users

    # eq comparison should fail now:
    assert users_old != users_transformed

    old_user_names = {user.value.name for user in users_old}
    transformed_user_names = {user.value.name for user in users_transformed}
    # manual set comparison should fail now:
    assert old_user_names != transformed_user_names

    # but the transformed names should match the uppercased old names:
    old_user_names = {name.upper() for name in old_user_names}
    assert old_user_names == transformed_user_names


def test_TransformValues_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata()
    users: RF = db.users

    # an item transformation_function that returns the item as is
    def transformation_function_new_instance(item: Item) -> Item | None:
        user: TF = item.value
        tf_new = TF()
        tf_new.name = user.name.upper()
        return Item(key=item.key, value=tf_new)

    transformed_values: Operator[RF, RF] = TransformValues[RF, RF](
        transformation_function=transformation_function_new_instance,
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

    # Wooaahh, this can be very confusing. Be careful when transforming values in place vs creating new instances!
    # The semantics of this must be clear. In its current form it is error-prone.
    # This requires a fundamental decision on how to handle such cases in FQL.
