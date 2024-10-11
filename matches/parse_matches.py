from typing import Dict
import json

def parse_match(match: Dict):
    final = {
        # general data
        'match_id': match['match_id'],
        'players': match['players'],
        'radiant_win': match['radiant_win'],
        #'start_time': match['start_time'],
        'duration': match['duration'],

        # bitmask integers that represent which towers are still standing when the game ended
        # 'barracks_status_dire': match['barracks_status_dire'],
        # 'barracks_status_radiant': match['barracks_status_radiant'],
        # 'tower_status_dire': match['tower_status_dire'],
        # 'tower_status_radiant': match['tower_status_radiant'],

        # kills when the match ended
        # 'dire_score': match['dire_score'],
        # 'radiant_score': match['radiant_score'],

        # radiant advantages (or disadvantages)
        # 'radiant_gold_adv': match['radiant_gold_adv'],
        # 'radiant_xp_adv': match['radiant_xp_adv'],
    }

    fields = [
        # general data
        'match_id',
        'players',
        'radiant_win',
        'start_time',
        'duration',

        # bitmask integers that represent which towers are still standing when the game ended
        'barracks_status_dire',
        'barracks_status_radiant',
        'tower_status_dire',
        'tower_status_radiant'

        # kills when the match ended
        'dire_score',
        'radiant_score',

        # radiant advantages (or disadvantages)
        'radiant_gold_adv',
        'radiant_xp_adv',

        # radiant and dire lineups
        'radiant_lineup',
        'dire_lineup'
    ]

    lineup_fields = [
    ]

if __name__ == "__main__":
    with open('matches_raw.json', 'r') as file:
        data = json.load(file)

    count = 0
    duplicates = 0
    cache = []

    for item in data:
        if item['match_id'] not in cache:
            cache.append(item['match_id'])
        else:
            duplicates += 1
        count += 1
    
    print(f'count: {count}')
    print(f'duplicates: {duplicates}')