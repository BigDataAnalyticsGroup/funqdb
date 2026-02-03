from fdm.python import TF, RF, DBF
from fql.operators.APIs import Operator
from fql.operators.filters import filter_items_scan, filter_items
from fql.util import Item
from tests.lib import _create_testdata, _subset_DBF


def test_filter_items():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(users)
    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_reused_and_chained():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(filter_RF(users))  # apply filter instance twice
    filter_RF(filter_RF(users)).explain()

    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_explain():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    print(users.get_lineage())

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    filter_RF2: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "bla",
        output_factory=lambda _: RF(),
    )

    ret1: RF = filter_RF(users, create_lineage=True)
    ret2: RF = filter_RF2(ret1, create_lineage=True)
    print("ret2 lineage:")
    lineage: list[str] = ret2.get_lineage()
    for i, lin in enumerate(lineage, 1):
        print(f"{i}.\t->", lin)


def test_filter_items_complement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate_complement(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    filter_RF: Operator[RF, RF] = filter_items_scan[RF, RF](
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
