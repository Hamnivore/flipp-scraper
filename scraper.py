import requests
import json
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import time

def get_headers():
    return {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.5',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'origin': 'https://flipp.com',
        'referer': 'https://flipp.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
        'connection': 'keep-alive',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'te': 'trailers'
    }

def get_flyer_ids(postal_code="84058"):
    url = f'https://cdn-gateflipp.flippback.com/bf/flipp/data?locale=en-us&postal_code={postal_code}&source=flipp_web'
    response = requests.get(url, headers=get_headers())
    data = json.loads(response.text)
    return data['flyers']

def get_flyer_items(flyer_id):
    url = f'https://flyers-ng.flippback.com/api/flipp/flyers/{flyer_id}/flyer_items'
    response = requests.get(url, headers=get_headers())
    response.raise_for_status()
    return response.json()

def download_all_flyers():
    data_dir = Path('flyer_data')
    data_dir.mkdir(exist_ok=True)
    
    flyers = get_flyer_ids()
    print(f"Found {len(flyers)} flyers")
    
    existing_flyer_ids = {file.stem.split('_')[-1] for file in data_dir.glob('*.json')}
    new_flyers = [flyer for flyer in flyers if str(flyer['id']) not in existing_flyer_ids]
    print(f"Found {len(new_flyers)} new flyers to download")
    
    for flyer in tqdm(new_flyers, desc="Downloading flyers"):
        try:
            items = get_flyer_items(flyer['id'])
            
            valid_from = datetime.fromisoformat(flyer['valid_from'].replace('Z', '+00:00')).strftime('%Y%m%d')
            valid_to = datetime.fromisoformat(flyer['valid_to'].replace('Z', '+00:00')).strftime('%Y%m%d')
            
            store_name = "".join(x for x in flyer['merchant'] if x.isalnum() or x in " -")
            flyer_name = "".join(x for x in flyer['name'] if x.isalnum() or x in " -")
            filename = f"{store_name}_{flyer_name}_{valid_from}-{valid_to}_{flyer['id']}.json"
            
            flyer_data = {
                'store': flyer['merchant'],
                'flyer_name': flyer['name'],
                'valid_from': flyer['valid_from'],
                'valid_to': flyer['valid_to'],
                'items': items
            }
            
            with open(data_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(flyer_data, f, indent=2)
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"\nError downloading flyer {flyer['id']}: {str(e)}")
            continue
    
    print("\nDownload complete!")
    print(f"Downloaded {len(new_flyers)} new flyers")
    print(f"Total flyers in {data_dir}: {len(list(data_dir.glob('*.json')))}")

if __name__ == "__main__":
    download_all_flyers()