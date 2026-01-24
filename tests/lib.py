from fql.functions import TF, RF, DBF


def _create_testdata(frozen: bool = False) -> DBF:
    """Creates test data for unit tests.
    @param frozen: Whether the created data structures should be frozen (read-only).
    @return: A database function (DBF) containing departments and users relations.
    """

    # departments relation:
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev", "budget": "11M"}, frozen),
            "d2": TF({"name": "Consulting", "budget": "22M"}, frozen),
        },
        frozen,
    )

    # users tuples:
    t1: TF = TF({"name": "Horst", "department": departments.d1}, frozen)
    t2: TF = TF({"name": "Tom", "department": departments.d1}, frozen)
    t3: TF = TF({"name": "John", "department": departments.d2}, frozen)
    # users relation:
    users: RF = RF({1: t1, 2: t2, 3: t3}, frozen)

    # customers tuples:
    c1: TF = TF({"name": "Tom", "company": "sample company"}, frozen)
    c2: TF = TF({"name": "Tom", "company": "example inc"}, frozen)
    c3: TF = TF({"name": "John", "company": "whatever gmbh"}, frozen)
    c4: TF = TF({"name": "Peter", "company": "Peter, Paul, and Mary"}, frozen)
    # customers relation:
    customers: RF = RF({1: c1, 2: c2, 3: c3, 4: c4}, frozen)

    # database of relations:
    db: DBF = DBF(
        {"departments": departments, "users": users, "customers": customers}, frozen
    )

    return db
