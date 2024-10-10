# script for getting match list from api
import asyncio
import aiohttp
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List

_match_list_dump: List[Dict] = []
_match_id_cache: List[str] = []
_json_path = 'ids.json'

# params
_quota = 2000
_matches_since = 1728086400
_min_rank = 70
_game_mode = 22
_url = f"https://api.opendota.com/api/publicMatches?min_rank={_min_rank}"

# params = {
#     "matches_since": 1728086400, # (since patch 7.37d)
#     "min_rank": 70, # (divine 1 and up)
#     "game_mode": 22, # (all_draft game mode / ranked)
#     "url": "https://api.opendota.com/api/publicMatches"
# }

async def fetch(session: aiohttp.ClientSession, url: str):
    async with session.get(url=url) as response:
        response.raise_for_status()
        if response.status == 200:
            body = await response.json()
            return body

async def fetch_loop(url: str):
    global _match_list_dump

    session = None
    try:
        session = aiohttp.ClientSession()
    except Exception as e:
        print(f'could not initiate a new aiohttp client session object: {str(e)}')
        return

    while len(_match_id_cache) < _quota:
        data: Any = None
        try:
            data = await fetch(session, url)
        except Exception as e:
            print(f'fetch() raised exception: {str(e)}')

        if data:
            if len(data) == 0:
                continue
            for match in data:
                try:
                    _match_list_dump.append(match)
                except Exception as e:
                    print(f'exception raised while appending match to match list: {str(e)}')
                    
        # 3 minute await to refresh list
        await asyncio.sleep(180)
    
    await session.close()

async def validator(start_time: int, game_mode: int, min_rank: int):
    global _match_list_dump
    global _match_id_cache

    count = 0

    with open(_json_path, 'r') as file:
        json_data = json.load(file)

    while len(_match_id_cache) < _quota:

        if len(_match_list_dump) > 0:
            for match in _match_list_dump:
                if match['match_id'] not in _match_id_cache:
                    if match['start_time'] < start_time:
                        continue
                    elif match['game_mode'] != game_mode:
                        continue
                    elif match['avg_rank_tier'] < min_rank:
                        continue
                    _match_id_cache.append(match['match_id'])
                    new_match = {
                        "match_id": match['match_id'],
                        "start_time": match['start_time'],
                        "game_mode": match['game_mode'],
                        "avg_rank_tier": match['avg_rank_tier']
                    }
                    json_data.append(new_match)
                    count += 1
                    print(f"({count}) -> {new_match}")
                
                _match_list_dump.remove(match)
            
            with open(_json_path, 'w') as file:
                json.dump(json_data, file, indent=4)
        
        await asyncio.sleep(1)
    
    print(f'match id list reached quota: {len(_match_id_cache)}')

async def collect():
    loop = asyncio.get_running_loop()
    match_list : List[Any] = []
    global _match_id_cache

    # init from ids.json
    with open(_json_path, 'r') as file:
        _ids = json.load(file)

    for m in _ids:
        _match_id_cache.append(m['match_id'])
    
    print(f"init match_id size: {len(_match_id_cache)}")

    if not loop:
        print('no loop')
        return

    try:
        tasks = []
        tasks.append(asyncio.create_task(fetch_loop(_url)))
        tasks.append(asyncio.create_task(validator(
                start_time=_matches_since,
                game_mode=_game_mode,
                min_rank=_min_rank
            )))
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f'could not create fetch_loop task: {str(e)}')

    # with open(_json_path, 'r') as file:
    #     _ids = json.load(file)
        
    #     for i in _ids:
    # TODO: implement duplication counting

    print('done... closing')

if __name__ == "__main__":
    asyncio.run(collect())