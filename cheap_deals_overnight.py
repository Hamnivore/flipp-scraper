import json
from pathlib import Path
from tqdm import tqdm
from openai import OpenAI
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from math import ceil

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI()

# Get the current date
current_date = datetime.now().strftime("%b %d, %Y")

# Set up directories
unlabeled_dir = Path('flyer_data')
labeled_dir = Path('labeled_flyer_data')
batch_dir = Path('batch_files')
labeled_dir.mkdir(exist_ok=True)
batch_dir.mkdir(exist_ok=True)

def log_info(message):
    """Log information to a file and print to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open('batch_processing_log.txt', 'a') as log_file:
        log_file.write(f"{timestamp} - {message}\n")
    print(message)

def cancel_all_batches():
    # Retrieve all batches
    batches = client.batches.list()
    # get the status of each batch
    batch_statuses = [client.batches.retrieve(batch.id) for batch in batches.data]
    # filter out the completed batches
    active_batches = [batch for batch in batch_statuses if batch.status != "completed" and batch.status != "failed"]
    
    log_info(f"Found {len(batches.data)} batches.")
    
    # Cancel each batch
    for batch in tqdm(active_batches, desc="Cancelling batches"):
        try:
            client.batches.cancel(batch.id)
            log_info(f"Cancelled batch {batch.id}")
        except Exception as e:
            log_info(f"Error cancelling batch {batch.id}: {str(e)}")
    
    log_info("Finished cancelling batches.")

system_prompt = """
Your goal is to help shoppers find the best deals. You will be given an image of a product ad and its price.
Calculate the price per unit and recommend what the user should do with the item. If at all possible,
get the unit as a weight or volume. Especially for food items, this is usually the best way to compare deals.
"""

def prepare_deal_input(item, flyer_filename):
    price = item.get('price')
    price_text = f"priced at ${float(price):.2f}" if price else "with no price listed"
    return {
        "custom_id": f"{flyer_filename}|{item['id']}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini-2024-07-18",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": f"Today is {current_date}. Evaluate {item['name']} {price_text}."},
                    {"type": "image_url", "image_url": {"url": item['cutout_image_url']}}
                ]}
            ],
            "max_tokens": 100,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "deal_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "price_per_unit": {
                                "description": "Formatted as $X.XX/unit, where unit is one of: lb, oz, g, kg, gal, qt, pt, fl oz, ea. If no price is available, use 'N/A'.",
                                "type": "string"
                            },
                            "user_recommendation": {
                                "description": "Tell the user what to do with this item so they can save money. For example, 'Buy bulk and fill your freezer!', 'Wait for a better deal', 'Buy if you need it soon', 'This deal is so good you should buy and resell it!'.",
                                "type": "string"
                            },
                            "deal_score": {
                                "description": "A score from 1 to 5 indicating how good the deal is. 1 is worse than average, 2 is around the expected price, and 5 is that rare smoking unicorn deal. If no price is available, use 0.",
                                "type": "integer",
                                "minimum": 0,
                                "maximum": 5
                            },
                            "meal_prep_score": {
                                "description": "A score from 1 to 5 indicating how good the deal is for meal-prepping. 1 is anything that isn't a food item, 2 is food that isn't very nutritious (soft drinks, candy, chips), 3 is high quality sweets (pastries, ice cream, etc.), 4 is good canned or frozen food, and 5 is fresh produce, meat, dairy, or other high quality food.",
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 5
                            },
                            "in_season": {
                                "description": "true if the item is in season, false otherwise.",
                                "type": "boolean"
                            }
                        },
                        "additionalProperties": False
                    }
                }
            }
        }
    }
def estimate_tokens(text, has_image=True):
    """Estimate the number of tokens in the given text, plus image tokens if applicable."""
    text_tokens = len(text) // 4  # Using the heuristic that 1 token ~= 4 chars in English
    image_tokens = 8500 if has_image else 0  # Add 8500 tokens for each image
    return text_tokens, image_tokens

def estimate_cost(text_tokens, image_tokens, max_output_tokens=100):
    """Estimate the cost based on input tokens, image tokens, and output tokens."""
    image_cost = (image_tokens / 1_000_000) * 0.15
    input_text_cost = (text_tokens / 1_000_000) * 0.075
    output_cost = (max_output_tokens / 1_000_000) * 0.300
    return image_cost + input_text_cost + output_cost

def load_and_prepare_data():
    batch_input = []
    flyers_to_process = []
    total_text_tokens = 0
    total_image_tokens = 0
    total_cost = 0
    MAX_TOKENS_PER_BATCH = 1_800_000

    labeled_flyer_ids = []
    for labeled_flyer_file in tqdm(list(labeled_dir.glob('*.json'))):
        labeled_flyer_id = labeled_flyer_file.stem.split('_')[-1]
        labeled_flyer_ids.append(labeled_flyer_id)


    for flyer_file in tqdm(list(unlabeled_dir.glob('*.json'))):
        with open(flyer_file, 'r') as f:
            flyer_data = json.load(f)
            
        flyer_id = flyer_file.stem.split('_')[-1]
        if flyer_id not in labeled_flyer_ids:
            flyers_to_process.append(flyer_file.name)
            
            for item in flyer_data['items']:
                deal_input = prepare_deal_input(item, flyer_file.name)
                batch_input.append(deal_input)
                
                text_tokens, image_tokens = estimate_tokens(json.dumps(deal_input))
                total_text_tokens += text_tokens
                total_image_tokens += image_tokens
                total_cost += estimate_cost(text_tokens, image_tokens)

    total_tokens = total_text_tokens + total_image_tokens
    num_batches = ceil(total_tokens / MAX_TOKENS_PER_BATCH)

    log_info(f"Loaded {len(flyers_to_process)} unlabeled flyers")
    log_info(f"Prepared {len(batch_input)} items for batch processing")
    log_info(f"Estimated total tokens: {total_tokens}")
    log_info(f"  - Text tokens: {total_text_tokens}")
    log_info(f"  - Image tokens: {total_image_tokens}")
    log_info(f"Estimated number of batches: {num_batches}")
    log_info(f"Estimated total cost: ${total_cost:.2f}")

    # Save flyers to process for later use
    with open('flyers_to_process.json', 'w') as f:
        json.dump(flyers_to_process, f)

    log_info("Flyers to process saved: flyers_to_process.json")

    # Save batch input for later use
    with open('batch_input.json', 'w') as f:
        json.dump(batch_input, f)

    log_info("Batch input saved: batch_input.json")

    return batch_input

def create_batch_files(batch_input):
    MAX_ITEMS_PER_BATCH = 200  # Adjust this value as needed

    batches = [batch_input[i:i + MAX_ITEMS_PER_BATCH] for i in range(0, len(batch_input), MAX_ITEMS_PER_BATCH)]

    # delete everything in the folder
    for file in batch_dir.glob('*'):
        file.unlink()

    for i, batch in enumerate(batches):
        batch_filename = batch_dir / f'batch_input_{i}.jsonl'
        with open(batch_filename, 'w') as f:
            for item in batch:
                f.write(json.dumps(item) + '\n')

    log_info(f"Created {len(batches)} batch files in {batch_dir}")

def process_batches():
    MAX_CONCURRENT_BATCHES = 5
    BATCH_CHECK_INTERVAL = 60

    def process_batch_group(batch_files):
        batch_ids = []
        for batch_file in batch_files:
            batch_input_file = client.files.create(
                file=open(batch_file, "rb"),
                purpose="batch"
            )

            log_info(f"Uploaded batch input file {batch_file.name} with ID: {batch_input_file.id}")

            batch = client.batches.create(
                input_file_id=batch_input_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
                metadata={
                    "description": f"Cheap deals analysis batch {batch_file.name}"
                }
            )

            batch_ids.append(batch.id)
            log_info(f"Created batch for {batch_file.name} with ID: {batch.id}")
        
        return batch_ids

    def wait_for_batches(batch_ids):
        active_batches = batch_ids.copy()
        all_results = []
        
        while active_batches:
            for batch_id in active_batches[:]:
                batch_status = client.batches.retrieve(batch_id)
                
                if batch_status.status == "completed":
                    output_file_content = client.files.content(batch_status.output_file_id)
                    output_text = output_file_content.read().decode('utf-8')
                    batch_results = [json.loads(line) for line in output_text.strip().split('\n')]
                    all_results.extend(batch_results)
                    active_batches.remove(batch_id)
                    log_info(f"Batch {batch_id} completed and processed.")
                elif batch_status.status == "failed":
                    log_info(f"Batch {batch_id} failed. Error: {batch_status.errors}")
                    active_batches.remove(batch_id)
            
            if active_batches:
                log_info(f"Waiting for {len(active_batches)} batches to complete...")
                time.sleep(BATCH_CHECK_INTERVAL)
        
        return all_results

    # Process batches in groups
    all_results = []
    batch_files = list(batch_dir.glob('batch_input_*.jsonl'))
    for i in range(0, len(batch_files), MAX_CONCURRENT_BATCHES):
        batch_group = batch_files[i:i+MAX_CONCURRENT_BATCHES]
        log_info(f"Processing batch group {i//MAX_CONCURRENT_BATCHES + 1} of {ceil(len(batch_files)/MAX_CONCURRENT_BATCHES)}")
        
        batch_ids = process_batch_group(batch_group)
        group_results = wait_for_batches(batch_ids)
        all_results.extend(group_results)
        
        log_info(f"Completed batch group {i//MAX_CONCURRENT_BATCHES + 1}. Total results so far: {len(all_results)}")

    # Save all results
    with open('completed_results.json', 'w') as f:
        json.dump(all_results, f, indent=2)

    log_info(f"All results saved to completed_results.json")
    return all_results

def update_flyers(all_results):
    def parse_response(response):
        if isinstance(response['body'], str):
            body = json.loads(response['body'])
        else:
            body = response['body']
        
        if isinstance(body['choices'][0]['message']['content'], str):
            content = json.loads(body['choices'][0]['message']['content'])
        else:
            content = body['choices'][0]['message']['content']
        
        return content

    # Group results by flyer
    flyer_results = {}
    for result in all_results:
        flyer_filename, item_id = result['custom_id'].split('|')
        if flyer_filename not in flyer_results:
            flyer_results[flyer_filename] = []
        flyer_results[flyer_filename].append((item_id, parse_response(result['response'])))

    # Update flyers with results and save labeled versions
    with open('flyers_to_process.json', 'r') as f:
        flyers_to_process = json.load(f)

    for flyer_filename in flyers_to_process:
        with open(unlabeled_dir / flyer_filename, 'r') as f:
            flyer_data = json.load(f)
        
        for item in flyer_data['items']:
            item_results = next((r for r in flyer_results[flyer_filename] if r[0] == str(item['id'])), None)
            if item_results:
                item['deal_analysis'] = item_results[1]
        
        # Save labeled flyer
        labeled_file = labeled_dir / flyer_filename
        with open(labeled_file, 'w') as f:
            json.dump(flyer_data, f, indent=2)

    log_info(f"Processed {len(all_results)} results and saved labeled flyers to {labeled_dir}")

def cleanup():
    os.remove('flyers_to_process.json')
    for batch_file in batch_dir.glob('batch_input_*.jsonl'):
        os.remove(batch_file)
    log_info("Cleaned up temporary files")

def print_sample():
    labeled_flyers = list(labeled_dir.glob('*.json'))
    if labeled_flyers:
        log_info("\nSample of labeled items:")
        sample_flyer = labeled_flyers[0]
        log_info(f"Flyer: {sample_flyer.name}")
        with open(sample_flyer, 'r') as f:
            flyer_data = json.load(f)
        for item in flyer_data['items'][:5]:
            log_info(json.dumps(item, indent=2))
            log_info("")
    else:
        log_info("No labeled flyers found. Please check if the processing completed successfully.")

def main():
    log_info("Starting cheap deals analysis")
    
    # Cancel any existing batches
    # cancel_all_batches()
    
    # Load and prepare data
    batch_input = load_and_prepare_data()
    
    # Create batch files
    create_batch_files(batch_input)
    
    # Process batches
    all_results = process_batches()
    
    # Update flyers with results
    update_flyers(all_results)
    
    # Cleanup temporary files
    cleanup()
    
    # Print sample of labeled items
    print_sample()
    
    log_info("Cheap deals analysis completed")

if __name__ == "__main__":
    main()