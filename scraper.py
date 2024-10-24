from curl_cffi import requests
import json
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import time
import os

# pip install curl-cffi tqdm pathlib

def get_flyer_ids(postal_code="84058"):
   url = f'https://cdn-gateflipp.flippback.com/bf/flipp/data?locale=en-us&postal_code={postal_code}&source=flipp_web'
   
   headers = {
       'accept': '*/*',
       'accept-language': 'en-US,en;q=0.9',
       'origin': 'https://flipp.com',
       'referer': 'https://flipp.com/',
       'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
       'sec-ch-ua-mobile': '?0',
       'sec-ch-ua-platform': '"Windows"',
       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
   }

   response = requests.get(url, headers=headers)
   data = json.loads(response.text)
   return data['flyers']

def get_flyer_items(flyer_id):
   url = f'https://flyers-ng.flippback.com/api/flipp/flyers/{flyer_id}/flyer_items'
   
   headers = {
       'accept': 'application/json',
       'accept-language': 'en-US,en;q=0.9',
       'origin': 'https://flipp.com',
       'referer': 'https://flipp.com/',
       'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
       'sec-ch-ua-mobile': '?0',
       'sec-ch-ua-platform': '"Windows"',
       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
   }

   response = requests.get(url, headers=headers)
   return json.loads(response.text)

def download_all_flyers():
    # Create data directory if it doesn't exist
    data_dir = Path('flyer_data')
    data_dir.mkdir(exist_ok=True)
    
    # Get all flyers
    flyers = get_flyer_ids()
    print(f"Found {len(flyers)} flyers")
    
    # Get existing flyer IDs
    existing_flyer_ids = set()
    for file in data_dir.glob('*.json'):
        flyer_id = file.stem.split('_')[-1]
        existing_flyer_ids.add(flyer_id)
    
    # Filter out already downloaded flyers
    new_flyers = [flyer for flyer in flyers if str(flyer['id']) not in existing_flyer_ids]
    print(f"Found {len(new_flyers)} new flyers to download")
    
    # Create progress bar
    pbar = tqdm(new_flyers, desc="Downloading flyers")
    
    for flyer in pbar:
        try:
            # Update progress bar description
            pbar.set_description(f"Downloading {flyer['merchant']} - {flyer['name']}")
            
            # Get flyer items
            items = get_flyer_items(flyer['id'])
            
            # Create a clean filename using store name, flyer name and date
            valid_from = datetime.fromisoformat(flyer['valid_from'].replace('Z', '+00:00')).strftime('%Y%m%d')
            valid_to = datetime.fromisoformat(flyer['valid_to'].replace('Z', '+00:00')).strftime('%Y%m%d')
            
            # Clean store and flyer names for filename
            store_name = "".join(x for x in flyer['merchant'] if x.isalnum() or x in " -")
            flyer_name = "".join(x for x in flyer['name'] if x.isalnum() or x in " -")
            filename = f"{store_name}_{flyer_name}_{valid_from}-{valid_to}_{flyer['id']}.json"
            
            # Combine flyer info with items
            flyer_data = {
                'store': flyer['merchant'],
                'flyer_name': flyer['name'],
                'valid_from': flyer['valid_from'],
                'valid_to': flyer['valid_to'],
                'items': [item for item in items if item['price'] and item['display_type'] == 1]
            }
            
            # Save to file
            with open(data_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(flyer_data, f, indent=2)
            
            # Small delay to be nice to their servers
            time.sleep(0.5)
            
        except Exception as e:
            print(f"\nError downloading flyer {flyer['id']}: {str(e)}")
            continue
    
    # Print summary
    print("\nDownload complete!")
    print(f"Downloaded {len(new_flyers)} new flyers")
    print(f"Total flyers in {data_dir}: {len(list(data_dir.glob('*.json')))}")


if __name__ == "__main__":
   download_all_flyers()