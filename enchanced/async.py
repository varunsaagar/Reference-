import json
import time
import pandas as pd
import asyncio
import aiohttp
from pydantic import BaseModel

def clean_prompt(prompt):
    """Clean the prompt string by removing trailing commas and whitespace."""
    if isinstance(prompt, str):
        return prompt.rstrip(',').strip()
    return prompt

async def vegas_async(session, prompt, request_number):
    """Asynchronously call Vegas API."""
    url = "https://vegas-llm-batch.verizon.com/vegas/apps/batch/prompt/LLMInsight"
    
    # Clean the prompt
    cleaned_prompt = clean_prompt(prompt)
    
    payload = {
        "useCase": "CALL_ANALYTICS_UI",
        "contextId": "CALL_INTENT_TEST",
        "preSeed_injection_map": {
            "{INPUT}": cleaned_prompt
        },
        "parameters": {
            "temperature": 0.9,
            "maxOutputTokens": 8192,
            "topP": 1
        }
    }
    
    # Debug: Print the exact payload being sent
    print(f"\nRequest {request_number} Payload:")
    print(json.dumps(payload, indent=2))
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        async with session.post(url, json=payload, headers=headers, ssl=False) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"\nRequest {request_number} failed with status {response.status}")
                print(f"Error response: {error_text}")
                return {'prediction': f"Error: Status {response.status}", 'index': request_number}
            
            response_data = await response.json()
            return {'prediction': response_data.get('prediction', ''), 'index': request_number}
            
    except Exception as ex:
        print(f"Request {request_number} failed with error: {ex}")
        return {'prediction': f"Error: {str(ex)}", 'index': request_number}

async def process_batch(session, df, start_idx, batch_size):
    """Process a batch of requests."""
    tasks = []
    for i in range(batch_size):
        if start_idx + i >= len(df):
            break
        prompt = df.iloc[start_idx + i]['prompt']
        # Debug: Print the prompt being processed
        print(f"\nProcessing prompt at index {start_idx + i}:")
        print(f"Original prompt: {prompt}")
        print(f"Cleaned prompt: {clean_prompt(prompt)}")
        task = asyncio.create_task(
            vegas_async(session, prompt, start_idx + i)
        )
        tasks.append(task)
    return await asyncio.gather(*tasks)

def run_async_requests_vegas(df, max_requests_per_minute):
    """Run async requests with rate limiting and save results."""
    async def process_requests():
        timeout = aiohttp.ClientTimeout(total=300)
        connector = aiohttp.TCPConnector(ssl=False, limit=max_requests_per_minute)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            results = []
            
            for batch_start in range(0, len(df), max_requests_per_minute):
                print(f"\nProcessing batch starting at index {batch_start}")
                batch_start_time = time.time()
                
                batch_results = await process_batch(
                    session, 
                    df, 
                    batch_start, 
                    max_requests_per_minute
                )
                
                results.extend(batch_results)
                
                elapsed = time.time() - batch_start_time
                if elapsed < 60 and batch_start + max_requests_per_minute < len(df):
                    wait_time = 60 - elapsed
                    print(f"Rate limiting: waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    
            return sorted(results, key=lambda x: x['index'])
            
    return asyncio.run(process_requests())

if __name__ == '__main__':
    file_name = "dummy.csv"
    
    # Read CSV and clean the data
    print("\nReading and cleaning CSV file contents:")
    df = pd.read_csv(file_name)
    df['prompt'] = df['prompt'].apply(clean_prompt)
    print(df.head())
    
    if 'id' not in df.columns or 'prompt' not in df.columns:
        print("Error: CSV file must contain 'id' and 'prompt' columns")
        exit(1)
    
    print(f"\nProcessing {len(df)} requests...")
    results = run_async_requests_vegas(df, 3)
    
    df['response'] = [r['prediction'] for r in results]
    
    df.to_csv(file_name, index=False)
    print(f"\nProcessed {len(results)} requests and saved results to {file_name}")
