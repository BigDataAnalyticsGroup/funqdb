from fdm.attribute_functions import DBF, RF
from fql.operators.partition import partition
from tests.lib import _create_testdata


def test_partitioning():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    partitions = partition(
        customers, partitioning_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom"
    ).result
    assert len(partitions) == 2
    assert type(partitions) == DBF

    tom_partition: RF = partitions["Tom"]
    assert type(tom_partition) == RF
    assert len(tom_partition) == 2
    for item in tom_partition:
        assert item.value.name == "Tom"

    not_tom_partition: RF = partitions["not Tom"]
    assert type(not_tom_partition) == RF
    assert len(not_tom_partition) == 3
    for item in not_tom_partition:
        assert item.value.name != "Tom"
