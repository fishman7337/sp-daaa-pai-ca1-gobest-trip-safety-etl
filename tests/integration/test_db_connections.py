import os

import pytest

pytestmark = pytest.mark.integration

if os.getenv("RUN_DB_TESTS") != "1":
    pytest.skip(
        "Set RUN_DB_TESTS=1 to run live database checks.", allow_module_level=True
    )

pymysql = pytest.importorskip("pymysql")

_missing_creds = [var for var in ("MYSQL_USER", "MYSQL_PASSWORD") if not os.getenv(var)]
if _missing_creds:
    pytest.skip(
        f"Missing required credentials: {', '.join(_missing_creds)}",
        allow_module_level=True,
    )


def _port(preferred_var):
    if os.getenv(preferred_var):
        return int(os.getenv(preferred_var))
    return int(os.getenv("MYSQL_PORT", 3306))


def _targets():
    targets = []

    staging_host = os.getenv("MYSQL_STAGING_HOST")
    if staging_host:
        targets.append(
            (
                "STAGING",
                staging_host,
                os.getenv("MYSQL_STAGING_DB", "staging"),
                _port("STAGING_DB_PORT"),
            )
        )

    warehouse_host = os.getenv("MYSQL_WAREHOUSE_HOST")
    if warehouse_host:
        targets.append(
            (
                "WAREHOUSE",
                warehouse_host,
                os.getenv("MYSQL_WAREHOUSE_DB", "warehouse"),
                _port("MYSQL_PORT"),
            )
        )

    if not targets:
        pytest.skip(
            "No DB hosts configured "
            "(set MYSQL_STAGING_HOST and/or MYSQL_WAREHOUSE_HOST).",
            allow_module_level=True,
        )

    return targets


@pytest.mark.parametrize("label,host,db_name,port", _targets())
def test_mysql_select_one(label, host, db_name, port):
    conn = pymysql.connect(
        host=host,
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=db_name,
        port=port,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            assert result is not None, f"{label} returned no result for SELECT 1"
    finally:
        conn.close()
