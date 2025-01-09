import json
import time
import pandas as pd
import asyncio
import aiohttp
from pydantic import BaseModel

class LLMParameters(BaseModel):
    """The class represents LLM Parameters to be passed to vegas api."""
    top_k: float = 1.0
    top_p: float = 1.0
    temperature: float = 0.9
    max_output_tokens: int = 4096

def clean_prompt(prompt):
    """Clean and validate the prompt."""
    if pd.isna(prompt):
        return ""
    return str(prompt).strip()

def parse_response_string(response):
    """Parse and clean the Vegas API response."""
    try:
        if not response or 'prediction' not in response:
            return "No prediction found"
            
        prediction = response['prediction']
        if not prediction:
            return "Empty prediction"
            
        prediction = str(prediction).strip()
        preduction
        
        return prediction
    except Exception as ex:
        print(f"Error parsing response: {ex}")
        return "Error parsing response"

async def vegas_async(session, prompt, request_number):
    """Asynchronously call Vegas API."""
    url = "https://vegas-llm-batch.verizon.com/vegas/apps/batch/prompt/LLMInsight"
    
    # Clean and validate prompt
    cleaned_prompt = clean_prompt(prompt)
    if not cleaned_prompt:
        return {'prediction': "Empty prompt", 'index': request_number}
    
    payload = {
        "useCase": "LLM_EVALUATION_FRAMEWORK",
        "contextId": "BILLING_SLM_EVAL",
        "preSeed_injection_map": {
            "{INPUT}": cleaned_prompt
        },
        "parameters": {
            "temperature": 0.9,
            "maxOutputTokens": 4096,
            "topP": 1.0,
            "topK": 1.0
        }
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        async with session.post(url, json=payload, headers=headers, ssl=False) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"Request {request_number} failed with status {response.status}: {error_text}")
                return {'prediction': f"Error: Status {response.status}", 'index': request_number}
                
            response_data = await response.json()
            parsed_response = parse_response_string(response_data)
            return {'prediction': parsed_response, 'index': request_number}
            
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
        task = asyncio.create_task(
            vegas_async(session, prompt, start_idx + i)
        )
        tasks.append(task)
    return await asyncio.gather(*tasks)

def run_async_requests_vegas(df, max_requests_per_minute):
    """Run async requests with rate limiting and save results."""
    async def process_requests():
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes timeout
        connector = aiohttp.TCPConnector(ssl=False, limit=max_requests_per_minute)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            results = []
            
            for batch_start in range(0, len(df), max_requests_per_minute):
                print(f"Processing batch starting at index {batch_start}")
                batch_start_time = time.time()
                
                batch_results = await process_batch(
                    session, 
                    df, 
                    batch_start, 
                    max_requests_per_minute
                )
                
                results.extend(batch_results)
                
                # Rate limiting
                elapsed = time.time() - batch_start_time
                if elapsed < 60 and batch_start + max_requests_per_minute < len(df):
                    wait_time = 60 - elapsed
                    print(f"Rate limiting: waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    
            return sorted(results, key=lambda x: x['index'])
            
    return asyncio.run(process_requests())

if __name__ == '__main__':
    try:
        # Read the CSV file
        file_name = "dummy.csv"
        df = pd.read_csv(file_name)
        
        # Validate DataFrame
        required_columns = ['id', 'prompt']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"CSV must contain columns: {required_columns}")
        
        # Clean data
        df['prompt'] = df['prompt'].fillna("")
        df['prompt'] = df['prompt'].astype(str).str.strip()
        
        print(f"Processing {len(df)} requests...")
        results = run_async_requests_vegas(df, 3)  # Reduced batch size to 3 for better control
        
        # Add responses to the DataFrame
        df['response'] = [r['prediction'] for r in results]
        
        # Save back to the same CSV file
        df.to_csv(file_name, index=False)
        print(f"Processed {len(results)} requests and saved results to {file_name}")
        
    except Exception as e:
        print(f"Error processing requests: {str(e)}")
