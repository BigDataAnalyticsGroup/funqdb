from fdm.functions import RF, DBF
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
