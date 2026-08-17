from pathlib import Path

from warehouse.deploy import parse_statements

DDL_DIR = Path(__file__).parent.parent / "warehouse" / "redshift_ddl"


def test_parse_statements_ignores_semicolons_inside_comment_prose():
    sql = """
    -- This comment mentions a semicolon; right there, mid-sentence.
    -- And another one here; see?
    CREATE TABLE foo (id INT);
    """
    statements = parse_statements(sql)
    assert statements == ["CREATE TABLE foo (id INT)"]


def test_parse_statements_drops_pure_comment_lines_and_blank_statements():
    sql = "-- just a comment\n\n-- another\n"
    assert parse_statements(sql) == []


def test_parse_statements_handles_multiple_real_statements():
    sql = "CREATE TABLE a (id INT); -- trailing comment\nCREATE TABLE b (id INT);"
    statements = parse_statements(sql)
    assert statements == ["CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"]


def test_schema_sql_parses_to_exactly_four_real_statements():
    content = (DDL_DIR / "schema.sql").read_text()
    statements = parse_statements(content)
    assert len(statements) == 4
    assert statements[0].startswith("CREATE SCHEMA")
    assert "dim_port" in statements[1]
    assert "dim_vessel_type" in statements[2]
    assert "fact_port_congestion_hourly" in statements[3]


def test_seed_files_parse_to_one_insert_each():
    for filename in ["seed_dim_port.sql", "seed_dim_vessel_type.sql"]:
        content = (DDL_DIR / filename).read_text()
        statements = parse_statements(content)
        assert len(statements) == 1
        assert statements[0].upper().startswith("INSERT INTO")
