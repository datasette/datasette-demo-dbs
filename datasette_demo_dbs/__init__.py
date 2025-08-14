from datasette import hookimpl
from datasette.database import Database
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Union, List, Dict
import logging
from pathlib import Path
import time

import asyncio
import httpx

_background_tasks = set()


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
def startup(datasette):

    loop = asyncio.get_running_loop()

    def loop_exception_handler(loop, context):
        # context contains 'message', 'exception', 'task', etc.
        logger.error("Asyncio loop error: %s", context.get("message"))
        exc = context.get("exception")
        if exc:
            logger.exception("Unhandled exception", exc_info=exc)

    loop.set_exception_handler(loop_exception_handler)

    async def inner():
        config = get_config(datasette)
        for demo_db in config.demo_dbs:
            # Does the DB exist already?
            db_path = config.path / (demo_db.name + ".db")
            if db_path.exists():
                continue
            # If it has been deliberately deleted do not fetch
            deleted_path = db_path.with_suffix(".deleted")
            if deleted_path.exists():
                continue

            # Fetch from the URL, using httpx
            def on_complete(result: DownloadResult):
                print("... downloaded", result)
                datasette.add_database(
                    Database(
                        datasette,
                        path=result.file_path,
                    ),
                    name=demo_db.name,
                )
            print("Gonna download:", demo_db)
            await download_file(
                demo_db.url, db_path.resolve(), block_seconds=0.5, on_complete=on_complete
            )

    return inner


@dataclass
class DownloadResult:
    url: str
    file_path: str
    started: bool = False
    completed: bool = False
    file_size: Optional[int] = None        # bytes
    error: Optional[str] = None
    download_time: Optional[float] = None  # seconds


async def download_file(
    url: str,
    file_path: str,
    block_seconds: float = 0.5,
    on_complete: Optional[Callable[[DownloadResult], None]] = None
) -> DownloadResult:
    print("download_file", url, file_path)
    start_time = time.time()
    result = DownloadResult(url, file_path)

    async def _download():
        print("_download()")
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()

                result.started = True
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)

                with open(file_path, 'wb') as f:
                    f.write(response.content)

                result.completed = True
                result.file_size = len(response.content)
                result.download_time = time.time() - start_time
                if on_complete:
                    on_complete(result)
        except Exception as e:
            result.error = str(e)
            result.completed = True
            result.download_time = time.time() - start_time
            if on_complete:
                on_complete(result)

    download_task = asyncio.create_task(_download())
    # We need to keep this around because otherwise it goes out of scope
    # and the task gets invisibly cancelled:
    _background_tasks.add(download_task)

    try:
        await asyncio.wait_for(download_task, timeout=block_seconds)
    except asyncio.TimeoutError:
        async def _background_completion():
            await download_task

        asyncio.create_task(_background_completion())

    return result
