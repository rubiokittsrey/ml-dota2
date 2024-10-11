# script for getting match list from api
import asyncio
import aiohttp
import json
from typing import Dict, Any, List

id_buffer: List[Dict] = []
id_cache: List[str] = []
json_path = 'ids.json'

async def fetch(session: aiohttp.ClientSession, url: str):
    async with session.get(url=url) as response:
        response.raise_for_status()
        return await response.json()

async def fetch_loop(url: str, quota: int):
    global id_buffer

    session = None
    try:
        session = aiohttp.ClientSession()
    except Exception as e:
        print(f'could not initiate a new aiohttp client session object: {str(e)}')
        return

    while len(id_cache) < quota:
        # 3 minute await to refresh list
        await asyncio.sleep(120)

        # get request
        data: Any = None
        try:
            data = await fetch(session, url)
        except aiohttp.ClientError as e:
            print(f'network error: {str(e)}')
        except asyncio.TimeoutError:
            print(f'request timed out')
        except Exception as e:
            print(f'fetch() raised unhandled exception: {str(e)}')
        
        # append to dump list
        if data:
            if len(data) == 0:
                print(f'fetch() returned with data but len {len(data)}')
                continue
            for match in data:
                id_buffer.append(match)
                    
    await session.close()

async def validator(start_time: int, game_mode: int, min_rank: int, quota: int):
    global id_buffer
    global id_cache
    count = 0

    with open(json_path, 'r') as file:
        json_data = json.load(file)

    while len(id_cache) < quota:

        # to json data and match id cache list
        if len(id_buffer) > 0:
            for match in id_buffer:

                # check for data eligibility
                if match['match_id'] in id_cache:
                    continue
                if match['start_time'] < start_time:
                    continue
                elif match['game_mode'] != game_mode:
                    continue
                elif match['avg_rank_tier'] < min_rank:
                    continue

                # store if ok
                id_cache.append(match['match_id'])
                new_match = {
                    "match_id": match['match_id'],
                    "start_time": match['start_time'],
                    "game_mode": match['game_mode'],
                    "avg_rank_tier": match['avg_rank_tier']
                }
                json_data.append(new_match)

                count += 1
                print(f"({count}) -> {new_match}")
                id_buffer.remove(match)
            
            with open(json_path, 'w') as file:
                json.dump(json_data, file, indent=4)
        
        await asyncio.sleep(1)
    
    print(f'match id list reached quota: {len(id_cache)}')

def dup_check():
    _count = 0

    with open(json_path, 'r') as file:
        _ids = json.load(file)
        _id_cache : List[str] = []
        _cache = []

        for i in _ids:
            if i['match_id'] not in _id_cache:
                _cache.append(i)
            else:
                _count += 1

    print(f"{_count} duplications found")
    
    # load no duplication copy to new .json if duplicates are found
    if _count == 0:
        return
    else:
        with open('ids_dupchecked.json') as file:
            json.dump(_cache, file, inden=4)

async def collect(url: str, matches_since: int, game_mode: int, min_rank: int, quota: int):
    global id_cache

    # init from ids.json
    with open(json_path, 'r') as file:
        _ids = json.load(file)

    for m in _ids:
        id_cache.append(m['match_id'])
    
    print(f"init match_id size: {len(id_cache)}")

    try:
        tasks = []
        tasks.append(asyncio.create_task(fetch_loop(url=url, quota=quota)))
        tasks.append(asyncio.create_task(validator(
                start_time=matches_since,
                game_mode=game_mode,
                min_rank=min_rank,
                quota=quota
            )))
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f'could not create fetch_loop task: {str(e)}')

    with open(json_path, 'r') as file:
        _ids = json.load(file)

    dup_check()

    print('done... closing')

if __name__ == "__main__":
    with open('../params.json', 'r') as f:
        params : Dict = json.load(f)

    # params
    quota = params['quota']
    matches_since = params['matches_since']
    min_rank = params['min_rank']
    game_mode = params['game_mode']
    url = f"https://api.opendota.com/api/publicMatches?min_rank={min_rank}"

    asyncio.run(collect(url, matches_since, game_mode, min_rank, quota))