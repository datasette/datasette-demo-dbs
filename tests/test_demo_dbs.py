from pathlib import Path
import pytest
import sqlite3
import httpx
from datasette.app import Datasette
from distutils.file_util import write_file

TEST_URL = "https://example.com/demo.db"

DEMO_DB_BYTES = (
    b"SQLite format 3"
    + bytes.fromhex(
        "00 10 00 01 01 00 40 20 20 00 00 00 01 00 00 00 02 00 00 00 00 00 00 "
        "00 00 00 00 00 01 00 00 00 04 00 00 00 00 00 00 00 00 00 00 00 01 00 "
        "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 "
        "00 00 00 00 00 00 00 00 00 00 00 01 00 2e 66 e9 0d 00 00 00 01 0f c0 "
        "00 0f c0"
    )
    + (b"\x00" * 3922)
    + bytes.fromhex("3e 01 06 17 15 15 01 61 74 61 62 6c 65 74 65 73 74 74 65 73 74 02")
    + b"CREATE TABLE test (id INTEGER PRIMARY KEY)"
    + bytes.fromhex("0d 00 00 00 00 10")
    + (b"\x00" * 4090)
)


@pytest.mark.asyncio
async def test_downloads_and_adds_database(tmp_path, httpx_mock):
    # remote returns some bytes for the .db
    httpx_mock.add_response(url=TEST_URL, content=DEMO_DB_BYTES)

    datasette = Datasette(
        [],
        memory=True,
        metadata={
            "plugins": {
                "datasette-demo-dbs": {
                    "path": str(tmp_path),
                    "dbs": {"demo": TEST_URL},
                }
            }
        },
    )

    await datasette.invoke_startup()

    db_file = tmp_path / "demo.db"
    assert db_file.exists()

    # Datasette should have a database registered with that name
    assert "demo" in datasette.databases
    # The Database object should point at the file we created
    db_obj = datasette.databases["demo"]
    # database path attribute is expected to include the filename
    assert str(db_file) in getattr(db_obj, "path", str(db_obj))
    # request should 200
    response = await datasette.client.get("/demo/test")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_skips_download_if_db_exists(tmp_path, httpx_mock):
    db_file = tmp_path / "demo.db"
    db_file.write_bytes(DEMO_DB_BYTES)

    datasette = Datasette(
        [],
        memory=True,
        metadata={
            "plugins": {
                "datasette-demo-dbs": {
                    "path": str(tmp_path),
                    "dbs": {"demo": TEST_URL},
                }
            }
        },
    )

    await datasette.invoke_startup()

    # No HTTP requests should have been made because file already existed
    assert len(httpx_mock.get_requests()) == 0

    # Table should be present
    response = await datasette.client.get("/demo/test")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_skips_download_if_deleted_marker_exists(tmp_path, httpx_mock):
    # Create a .deleted marker
    deleted = tmp_path / "demo.deleted"
    deleted.write_text("deleted")

    datasette = Datasette(
        [],
        memory=True,
        metadata={
            "plugins": {
                "datasette-demo-dbs": {
                    "path": str(tmp_path),
                    "dbs": {"demo": TEST_URL},
                }
            }
        },
    )

    await datasette.invoke_startup()

    # No HTTP requests should have been made because .deleted exists
    assert len(httpx_mock.get_requests()) == 0
    # No .db file should have been created
    assert not (tmp_path / "demo.db").exists()
