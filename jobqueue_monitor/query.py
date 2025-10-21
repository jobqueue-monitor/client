from __future__ import annotations

from typing import Any

import httpx


async def query(local_port: int, kind: str) -> dict[str, Any]:
    url = f"http://localhost:{local_port}/query/{kind}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            return {}


async def shutdown(local_port: int) -> None:
    url = f"http://localhost:{local_port}/shutdown"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
