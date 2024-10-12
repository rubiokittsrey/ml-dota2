from typing import Dict, List, Tuple
import json

json_path = 'simple_match_set.json'
raw_set = '../matches_raw/matches_raw.json'

def parse_lineups(players: List) -> Tuple[
    List[int], List[int]]:
    dire = []
    radiant = []

    for p in players:
        if 0 <= p['player_slot'] <= 127:
            radiant.append(p['hero_id'])
        else:
            dire.append(p['hero_id'])
    
    radiant.sort()
    dire.sort()

    return radiant, dire

def parse_match(match: Dict) -> Dict:
    # hero lineups
    radiant, dire = parse_lineups(match['players'])

    try:
        final = {
            # general data
            #'match_id': match['match_id'],

            # scores
            'radiant_score': match['radiant_score'],
            'dire_score': match['dire_score'],

            # lineup
            'radiant_lineup': radiant,
            'dire_lineup': dire,

            # target
            'radiant_win': match['radiant_win'],
        }
    except KeyError:
        raise

    return final

if __name__ == "__main__":
    with open(raw_set, 'r') as file:
        data = json.load(file)

    count = 0
    duplicates = 0
    cache = []

    with open(json_path, 'r') as file:
        json_data = json.load(file)
    
    for item in data:
        if item['match_id'] not in cache:
            cache.append(item['match_id'])
        else:
            duplicates += 1
        count += 1

    print(f'count: {count}')
    print(f'duplicates: {duplicates}')

    failed = 0
    success = 0
    for item in data:
        try:
            p = parse_match(item)
            json_data.append(p)
            success += 1
        except KeyError:
            failed += 1

    with open(json_path, 'w') as file:
        json.dump(json_data, file, indent=4)

    print(f'failed: {failed}')
    print(f'sucess: {success}')