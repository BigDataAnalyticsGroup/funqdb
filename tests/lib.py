from fql.functions import TF, RF, DBF
from fql.operators.filters import filter_items


def _create_testdata(frozen: bool = False, observe_values: bool = False) -> DBF:
    """Creates test data for unit tests.
    @param frozen: Whether the created data structures should be frozen (read-only).
    @return: A database function (DBF) containing departments and users relations.
    """

    # departments tuples and relation:
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev", "budget": "11M"}, frozen),
            "d2": TF({"name": "Consulting", "budget": "22M"}, frozen),
        },
        frozen=frozen,
        observe_values=observe_values,
    )

    # users tuples and relation:
    users: RF = RF(
        {
            1: TF({"name": "Horst", "yob": 1972, "department": departments.d1}, frozen),
            2: TF({"name": "Tom", "yob": 1983, "department": departments.d1}, frozen),
            3: TF({"name": "John", "yob": 2002, "department": departments.d2}, frozen),
        },
        frozen=frozen,
        observe_values=observe_values,
    )

    # customers tuples and relation:
    customers: RF = RF(
        {
            1: TF({"name": "Tom", "company": "sample company"}, frozen),
            2: TF({"name": "Tom", "company": "example inc"}, frozen),
            3: TF({"name": "John", "company": "whatever gmbh"}, frozen),
            4: TF({"name": "Peter", "company": "Peter, Paul, and Mary"}, frozen),
            5: TF({"name": "Frank", "company": "Masterhorst"}, frozen),
        },
        frozen=frozen,
        observe_values=observe_values,
    )

    # database of relations:
    db: DBF = DBF(
        {"departments": departments, "users": users, "customers": customers},
        frozen,
        observe_values=observe_values,
    )

    return db


def _users_customers_DBF(frozen: bool = True) -> DBF:
    return filter_items(lambda i: i.key in ["users", "customers"], lambda _: DBF())(
        _create_testdata(frozen=frozen)
    )


def _subset_DBF(whitelist: set[str], frozen: bool = True) -> DBF:
    return filter_items(lambda i: i.key in whitelist, lambda _: DBF())(
        _create_testdata(frozen=frozen)
    )
