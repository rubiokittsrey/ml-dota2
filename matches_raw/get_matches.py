# script for getting match details of matches from match_ids dataset
import os
import asyncio
import aiohttp
import json
import requests
import time
from typing import Dict, Any, List, Tuple

match_buffer: List[Dict] = []
id_cache: List[int] = []
id_done_cache: List[int] = []
json_path = 'matches_raw.json'
source_path = '../match_ids/ids.json'

async def fetch(session: aiohttp.ClientSession, url: str):
    async with session.get(url=url) as response:
        response.raise_for_status()
        return await response.json()

async def fetch_loop(url: str, quota: int):
    global id_cache
    global match_buffer

    session = None
    try:
        session = aiohttp.ClientSession()
    except Exception as e:
        print(f'could not initiate a new aiohttp client session object: {str(e)}')
        return

    count = 0
    await_c = 0
    for i in id_cache:
        data: Any = None
        count += 1
        if i in id_done_cache:
            continue
        await_c += 1

        try:
            data = await fetch(session, f"{url}{i}")
        except aiohttp.ClientError as e:
            print(f'network error: {str(e)}')
        except asyncio.TimeoutError:
            print(f'request timed out')
        except Exception as e:
            print(f'fetch() raised unhandled exception: {str(e)}')

        if data:
            try:
                print(f"fetch() got data: id -> {data['match_id']}")
            except KeyError:
                pass
            match_buffer.append(data)

        if count == quota:
            print(f'fetch_loop reached quota')
            return

        if await_c == 10:
            await_c = 0
            await asyncio.sleep(30)

async def write_to_json(quota: int):
    global match_buffer
    count = 0
    failed = 0

    with open(json_path, 'r') as file:
        json_data : List[Dict] = json.load(file)

    while count < quota:
        if len(match_buffer) > 0:
                
                for match in match_buffer:
                    if list(match.keys()).count('version') > 1:
                        continue
                    try:
                        json_data.append(dict(match))
                        match_buffer.remove(match)
                        count += 1
                        print(f'({count}) data added to json_data')
                    except TypeError as e:
                        failed += 1
                        print(f'type error raised: {str(e)}')

                with open(json_path, 'w') as file:
                    json.dump(json_data, file, indent=4)

        await asyncio.sleep(1)

    print(f'added matches to {json_path} (count: {count}, failed: {failed})')

async def run(url:str):
    global id_cache

    with open(source_path, 'r') as file:
        entries = json.load(file)
    for i in entries:
        id_cache.append(i['match_id'])
    
    with open(json_path, 'r') as file:
        entries = json.load(file)
    for i in entries:
        id_done_cache.append(i['match_id'])

    quota = len(id_cache)
    print(f'match_id count from {source_path}: {len(id_cache)}')
    print(f'match_id count from {json_path}: {len(id_done_cache)}')
    print(f'quota count: {quota}')

    try:
        tasks = []
        tasks.append(asyncio.create_task(fetch_loop(url=url, quota=quota)))
        tasks.append(asyncio.create_task(write_to_json(quota=quota)))
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f'unhandled exception: {str(e)}')

    print('done... closing')

if __name__ == "__main__":
    url = 'https://api.opendota.com/api/matches/'
    asyncio.run(run(url))