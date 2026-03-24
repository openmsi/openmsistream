import pathlib
import datetime
from dataclasses import dataclass, fields
from typing import List, Set, Tuple

import pytest

from openmsistream.utilities.dataclass_table import DataclassTable


@dataclass
class TableLineForTesting:
    str_field: str
    int_field: int
    float_field: float
    complex_field: complex
    bool_field: bool
    list_strs_field: List[str]
    set_ints_field: Set[int]
    tuple_floats_field: Tuple[float]
    path_field: pathlib.Path
    timestamp_field: datetime.datetime
    weird_field: str


def test_dataclass_table_functions(output_dir, logger):
    # ---- entries ----
    entry_1 = TableLineForTesting(
        "test",
        1,
        2.3,
        4 + 5j,
        True,
        ["str 1", "str 2", "str 3"],
        {1, 2, 3},
        (6.7, 8.9),
        pathlib.Path(__file__),
        datetime.datetime.now(),
        "asdf;foo",
    )
    entry_2 = TableLineForTesting(
        "test",
        2,
        11.12,
        13 - 14j,
        False,
        ["str 15", "str 16"],
        {17, 18, 19},
        (20.21, 22.23),
        pathlib.Path(__file__).parent,
        datetime.datetime.now(),
        "\tC:\\Path\\With\\Tab\\and,commas.txt",
    )
    entry_3 = TableLineForTesting(
        "test",
        3,
        24.34,
        6 + 2j,
        True,
        ["hello", "hi", "hey"],
        {25, 35, 43},
        (72.0, 84.6),
        pathlib.Path(__file__).parent.parent,
        datetime.datetime.now(),
        r"C:\Users\Name\Documents\Report.pdf",
    )

    entry_1_addr = hex(id(entry_1))
    entry_2_addr = hex(id(entry_2))
    entry_3_addr = hex(id(entry_3))

    table_path = output_dir / "test_table.csv"

    # ---- initial state ----
    assert not table_path.is_file()

    test_table = DataclassTable(
        dataclass_type=TableLineForTesting,
        filepath=table_path,
        logger=logger,
    )

    # ---- add/remove/add ----
    test_table.add_entries(entry_1)
    test_table.remove_entries(entry_1_addr)
    test_table.add_entries(entry_1)

    # ---- check attrs ----
    entry_1_attrs_read = test_table.get_entry_attrs(entry_1_addr)
    for f in fields(entry_1):
        assert getattr(entry_1, f.name) == entry_1_attrs_read[f.name]

    # ---- five expected exceptions ----
    with pytest.raises(ValueError):
        test_table.add_entries(entry_1)

    with pytest.raises(ValueError):
        test_table.remove_entries(entry_2_addr)

    with pytest.raises(ValueError):
        test_table.get_entry_attrs(entry_2_addr)

    with pytest.raises(ValueError):
        test_table.set_entry_attrs(entry_2_addr, str_field="this should break")

    with pytest.raises(ValueError):
        test_table.obj_addresses_by_key_attr("not_a_dataclass_attribute_name")

    # ---- dump & reload ----
    test_table.dump_to_file()

    test_table = DataclassTable(
        dataclass_type=TableLineForTesting,
        filepath=table_path,
        logger=logger,
    )

    # ---- verify entries ----
    assert len(test_table.obj_addresses_by_key_attr("str_field")["test"]) == 1

    test_table.add_entries([entry_2, entry_3])
    assert len(test_table.obj_addresses_by_key_attr("str_field")["test"]) == 3

    assert len(test_table.obj_addresses_by_key_attr("int_field")) == 3
    assert list(test_table.obj_addresses_by_key_attr("weird_field").keys()) == [
        _.weird_field for _ in [entry_1, entry_2, entry_3]
    ]

    # ---- modify entry_2 ----
    test_table.set_entry_attrs(entry_2_addr, str_field="testing")
    by_str_field = test_table.obj_addresses_by_key_attr("str_field")

    assert "test" in by_str_field
    assert len(by_str_field["test"]) == 2
    assert entry_3_addr in by_str_field["test"]

    assert "testing" in by_str_field
    assert len(by_str_field["testing"]) == 1
    assert entry_2_addr in by_str_field["testing"]

    test_table.dump_to_file()

    # append empty line to the end of the file
    with open(table_path, "a") as f:
        f.write("\n\t\n")

    # ---- reload & verify it fails ----
    with pytest.raises(RuntimeError) as exc_info:
        test_table = DataclassTable(
            dataclass_type=TableLineForTesting,
            filepath=table_path,
            logger=logger,
        )
        assert "failed to parse line from csv file" in str(exc_info.value)
