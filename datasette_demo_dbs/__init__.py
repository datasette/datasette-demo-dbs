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
        if db_path.exists():
            continue
        deleted_path = db_path.with_suffix(".deleted")
        if deleted_path.exists():
            continue

        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(demo_db.url)
            response.raise_for_status()

        db_path.parent.mkdir(parents=True, exist_ok=True)
        with open(db_path, "wb") as f:
            f.write(response.content)

        datasette.add_database(
            Database(
                datasette,
                path=str(db_path),
            ),
            name=demo_db.name,
        )
