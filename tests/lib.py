from fql.functions import TF, RF, DBF


def _create_testdata():
    """Creates test data for unit tests."""

    # departments tuples:
    d1: TF = TF({"name": "Dev", "budget": "11M"})
    d2: TF = TF({"name": "Consulting", "budget": "22M"})
    # departments relation:
    departments: RF = RF({"d1": d1, "d2": d2})

    # users tuples:
    t1: TF = TF({"name": "Horst", "department": d1})
    t2: TF = TF({"name": "Tom", "department": d1})
    t3: TF = TF({"name": "John", "department": d2})
    # users relation:
    users: RF = RF({1: t1, 2: t2, 3: t3})

    # database of relations:
    db: DBF = DBF({"departments": departments, "users": users})
    return db
