from fdm.python import RF, DBF
from fql.operators.subdatabases import subdatabase
from tests.lib import _users_customers_DBF


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
