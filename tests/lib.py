from fql.functions import TF, RF, DBF


def _create_testdata(read_only: bool = False) -> DBF:
    """Creates test data for unit tests."""

    # departments tuples:
    d1: TF = TF({"name": "Dev", "budget": "11M"}, read_only)
    d2: TF = TF({"name": "Consulting", "budget": "22M"}, read_only)
    # departments relation:
    departments: RF = RF({"d1": d1, "d2": d2}, read_only)

    # users tuples:
    t1: TF = TF({"name": "Horst", "department": d1}, read_only)
    t2: TF = TF({"name": "Tom", "department": d1}, read_only)
    t3: TF = TF({"name": "John", "department": d2}, read_only)
    # users relation:
    users: RF = RF({1: t1, 2: t2, 3: t3}, read_only)

    # database of relations:
    db: DBF = DBF({"departments": departments, "users": users}, read_only)
    return db
