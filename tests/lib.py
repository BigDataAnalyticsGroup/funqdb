from fql.functions import TF, RF, DBF


def _create_testdata(frozen: bool = False) -> DBF:
    """Creates test data for unit tests.
    @param frozen: Whether the created data structures should be frozen (read-only).
    @return: A database function (DBF) containing departments and users relations.
    """

    # departments tuples:
    d1: TF = TF({"name": "Dev", "budget": "11M"}, frozen)
    d2: TF = TF({"name": "Consulting", "budget": "22M"}, frozen)
    # departments relation:
    departments: RF = RF({"d1": d1, "d2": d2}, frozen)

    # users tuples:
    t1: TF = TF({"name": "Horst", "department": d1}, frozen)
    t2: TF = TF({"name": "Tom", "department": d1}, frozen)
    t3: TF = TF({"name": "John", "department": d2}, frozen)
    # users relation:
    users: RF = RF({1: t1, 2: t2, 3: t3}, frozen)

    # database of relations:
    db: DBF = DBF({"departments": departments, "users": users}, frozen)

    return db
