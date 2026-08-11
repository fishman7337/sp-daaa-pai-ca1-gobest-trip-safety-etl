import os

import pytest

pytestmark = pytest.mark.integration

if os.getenv("RUN_DB_TESTS") != "1":
    pytest.skip(
        "Set RUN_DB_TESTS=1 to run live database checks.", allow_module_level=True
    )

pymysql = pytest.importorskip("pymysql")

# Fail early if required credentials are missing
_missing_required = [
    var for var in ("MYSQL_USER", "MYSQL_PASSWORD") if not os.getenv(var)
]
if _missing_required:
    pytest.skip(
        f"Missing required env vars: {', '.join(_missing_required)}",
        allow_module_level=True,
    )


def _connection_params():
    """Build (label, host, db_name) tuples from env; skip if none configured."""
    params = []

    staging_host = os.getenv("MYSQL_STAGING_HOST")
    warehouse_host = os.getenv("MYSQL_WAREHOUSE_HOST")

    if staging_host:
        params.append(
            ("STAGING", staging_host, os.getenv("MYSQL_STAGING_DB", "staging"))
        )
    if warehouse_host:
        params.append(
            ("WAREHOUSE", warehouse_host, os.getenv("MYSQL_WAREHOUSE_DB", "warehouse"))
        )

    if not params:
        pytest.skip(
            "No database hosts configured "
            "(set MYSQL_STAGING_HOST and/or MYSQL_WAREHOUSE_HOST).",
            allow_module_level=True,
        )

    return params


@pytest.mark.parametrize("label,host,db_name", _connection_params())
def test_db_connection(label, host, db_name):
    port = int(os.getenv("MYSQL_PORT", 3306))
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


if __name__ == "__main__":
    # Allow running directly without pytest
    for label, host, db_name in _connection_params():
        print(f"\nTesting {label} connection to {host}/{db_name}...")
        test_db_connection(label, host, db_name)
        print(f"{label} OK")
