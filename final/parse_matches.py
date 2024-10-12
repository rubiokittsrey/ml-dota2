from typing import Dict, List, Tuple
from hashlib import sha256
import random
import json

json_path = 'dataset.json'
raw_set = '../matches_raw/matches_raw.json'

# the final dataset shape:
# [
# 'radiant' : list,
# 'dire' : list
# ]

def lineups_as_float(rad: List, dire: List, min_val=0, max_val=1.0) -> Tuple[float, float]:
    _r: int = 0
    _d: int = 0

    cache = [rad.sort(), dire.sort()]
    final: List[int] = []

    # generate hash from stringified list
    # generate int seed from hash
    # generated random float with seed
    for l in cache:
        arr_hash = sha256(str(l).encode()).hexdigest()
        seed = int(arr_hash, 16)
        random.seed(seed)
        final.append(random.uniform(min_val, max_val))

    return final[0], final[1] # radiant, dire

def parse_match(match: Dict) -> Dict:
    radiant: List[Dict] = []
    dire: List[Dict] = []
    
    r_lineup: List[int] = []
    d_lineup: List[int] = []

    for player in match['players']:
        p_buffer = {
            'hero_id': player['hero_id'],
            # performance (raw)
            # 'kda': player['kda'],
            # 'kills_per_min': player['kills_per_min'],
            # 'gold_per_min': player['gold_per_min'],
            # 'xp_per_min': player['xp_per_min'],

            # benchmarks (percentile)
            'kills_per_min_pct': player['benchmarks']['kills_per_min']['pct'],
            'gold_per_min_pct': player['benchmarks']['gold_per_min']['pct'],
            'xp_per_min_pct': player['benchmarks']['xp_per_min']['pct'],
            'last_hits_per_min_pct': player['benchmarks']['last_hits_per_min']['pct'],

            # damage/healing outputs (raw)
            # 'hero_damage': player['hero_damage'],
            # 'tower_damage': player['tower_damage'],
            # 'hero_healing': player['hero_healing'],

            # damage/healing outputs (percentiles)
            'hero_dmg_per_min_pct': player['benchmarks']['hero_damage_per_min']['pct'],
            'tower_dmg_pct': player['benchmarks']['tower_damage']['pct'],
            'hero_hl_per_min_pct': player['benchmarks']['hero_healing_per_min']['pct'],
        }

        if bool(player['isRadiant']):
            radiant.append(p_buffer)
        else:
            dire.append(p_buffer)

    for player in match['players']:
        if bool(player['isRadiant']):
            r_lineup.append(player['hero_id'])
        else:
            d_lineup.append(player['hero_id'])

    r_int, d_int = lineups_as_float(rad=r_lineup, dire=d_lineup)

    final = {
        'radiant': sorted(radiant, key=lambda m: m['hero_id']),
        'radiant_lineup': r_int,
        'dire': sorted(dire, key=lambda m: m['hero_id']),
        'dire_lineup': d_int,
        'win': int(match['radiant_win']), # 1 = radiant, 0 = dire
        'duration': match['duration']
    }

    return final

if __name__ == '__main__':
    with open(raw_set, 'r') as file:
        raw_data = json.load(file)

    with open(json_path, 'r') as file:
        json_data: List[Dict] = json.load(file)
    
    print(f'raw_data entries count: {len(raw_data)}')

    failed = 0
    count = 0
    duplicates = 0
    cache = []

    for m in raw_data:
        try:
            parsed = parse_match(m)
            json_data.append(parsed)
            count += 1
        except KeyError as e:
            failed += 1
            print(f'key error: {str(e)}')
    
    with open(json_path, 'w') as file:
        json.dump(json_data, file, indent=4)

    print(f'success: {count}')
    print(f'failed: {failed}')