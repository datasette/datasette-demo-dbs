from datasette import hookimpl
from datasette.database import Database
from dataclasses import dataclass
from typing import List
from pathlib import Path
import httpx


@dataclass
class DemoDB:
    name: str
    url: str


@dataclass
class Config:
    path: Path
    demo_dbs: List[DemoDB]


def get_config(datasette) -> Config:
    config = datasette.plugin_config("datasette-demo-dbs") or {}
    path = Path(config.get("path") or ".")
    dbs = config.get("dbs") or {}
    demo_dbs = [DemoDB(name=name, url=url) for name, url in dbs.items()]
    return Config(path=path, demo_dbs=demo_dbs)


@hookimpl
async def startup(datasette):
    config = get_config(datasette)
    for demo_db in config.demo_dbs:
        db_path = config.path / (demo_db.name + ".db")
        deleted_path = db_path.with_suffix(".deleted")
        if not db_path.exists() and not deleted_path.exists():
            # Download and save the database
            db_path.parent.mkdir(parents=True, exist_ok=True)
            async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
                async with client.stream("GET", demo_db.url) as response:
                    response.raise_for_status()
                    with open(db_path, "wb") as fp:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            fp.write(chunk)
        if db_path.exists() and demo_db.name not in datasette.databases:
            datasette.add_database(
                Database(
                    datasette,
                    path=str(db_path),
                ),
                name=demo_db.name,
            )
