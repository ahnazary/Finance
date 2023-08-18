from pytest import fixture
from sqlalchemy import create_engine, text


@fixture
def setup_tables(test_engine):
    """
    Fixture to set up the tables in the database
    """
    # execute schema.sql
    with open("finance/tests/schemas.sql", "r") as f:
        query = f.read()
    with test_engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()


@fixture
def test_engine():
    """
    Test to check if the engine is created correctly
    """
    engine = create_engine("postgresql://postgres:postgres@localhost:5438/postgres")
    yield engine

    # drop tables
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS stocks CASCADE"))
        conn.commit()
