from streaming.table_schema import (
    COLUMNS,
    derive_partition_values,
    glue_columns,
    glue_partition_keys,
    pyarrow_schema,
)


def test_glue_columns_match_column_definitions():
    cols = glue_columns()
    assert [c["Name"] for c in cols] == [name for name, _, _ in COLUMNS]
    assert [c["Type"] for c in cols] == [glue_type for _, glue_type, _ in COLUMNS]


def test_pyarrow_schema_has_same_field_order_as_columns():
    schema = pyarrow_schema()
    assert schema.names == [name for name, _, _ in COLUMNS]


def test_glue_partition_keys_are_port_zone_date_hour():
    keys = [k["Name"] for k in glue_partition_keys()]
    assert keys == ["port_zone", "date", "hour"]


def test_derive_partition_values_normal_record():
    record = {"port_zone": "USLAX", "timestamp": "2026-08-17T15:02:45.321601+00:00"}
    assert derive_partition_values(record) == ("USLAX", "2026-08-17", "15")


def test_derive_partition_values_missing_port_zone_buckets_as_unknown():
    record = {"port_zone": None, "timestamp": "2026-08-17T00:00:00+00:00"}
    assert derive_partition_values(record) == ("UNKNOWN", "2026-08-17", "00")
