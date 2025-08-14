from pathlib import Path
import pytest
import httpx
from datasette.app import Datasette

TEST_URL = "https://example.com/demo.db"


@pytest.mark.asyncio
async def test_downloads_and_adds_database(tmp_path, httpx_mock):
    # remote returns some bytes for the .db
    httpx_mock.add_response(url=TEST_URL, content=b"FAKE SQL LITE BYTES")

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


@pytest.mark.asyncio
async def test_skips_download_if_db_exists(tmp_path, httpx_mock):
    # Create an existing .db file before startup
    db_file = tmp_path / "demo.db"
    db_file.write_bytes(b"ALREADY THERE")

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

    # File should remain unchanged
    assert db_file.read_bytes() == b"ALREADY THERE"


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
