
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

def parse_response_string(response):
    """Parse and clean the Vegas API response."""
    try:
        prediction = response.get('prediction', '')
        if not prediction:
            return "No prediction found"
            
        prediction = prediction.strip()
        prediction = prediction.replace("```
json", "").replace("

        
        if prediction.startswith('{{') and prediction.endswith('}}'):
            prediction = prediction[1:-1].strip()
            
        return prediction
    except Exception as ex:
        print(f"Error parsing response: {ex}")
        return "Error parsing response"

async def vegas_async(session, prompt, request_number):
    """Asynchronously call Vegas API."""
    url = "https://vegas-llm-batch.verizon.com/vegas/apps/batch/prompt/LLMInsight"
    
    payload = {
        "useCase": "CALL_ANALYTICS_UI",
        "contextId": "CALL_INTENT_TEST",
        "preSeed_injection_map": {
            "{INPUT}": prompt
        },
        "parameters": {
            "temperature": 0.9,
            "maxOutputTokens": 4096,
            "topP": 1,
            "topK": 1
        }
    }
    
    headers = {'Content-Type': 'application/json'}
    
    try:
        async with session.post(url, json=payload, headers=headers, ssl=False) as response:
            if response.status != 200:
                print(f"Request {request_number} failed with status {response.status}")
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
        async with aiohttp.ClientSession() as session:
            results = []
            
            for batch_start in range(0, len(df), max_requests_per_minute):
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
                    await asyncio.sleep(60 - elapsed)
                    
            # Sort results by index to maintain original order
            sorted_results = sorted(results, key=lambda x: x['index'])
            return [r['prediction'] for r in sorted_results]
            
    return asyncio.run(process_requests())

if __name__ == '__main__':
    # Read the CSV file
    file_name = "dummy.csv"
    df = pd.read_csv(file_name)
    
    # Process the requests and get responses
    responses = run_async_requests_vegas(df, 10)
    
    # Add responses to the DataFrame
    df['response'] = responses
    
    # Save back to the same CSV file
    df.to_csv(file_name, index=False)
    print(f"Processed {len(responses)} requests and saved results to {file_name}")


payload_dict = { "useCase":c_usecase, "contextId": context_id, "preSeed_injection_map": context, "parameters": parameters } if gemini_flash: payload_dict['transactionMetadata'] = {"clientId": "1234" } 

this is the sample payload , api key is not required since endpoint is not authenticaked and open fix the issue # api_key = 'ehG4iYbAcujzXjP6AXG2GAhq2heMR7wS' payload = json.dumps(payload_dict) headers = { 'Content-Type': 'application/json', 'X-apikey': api_key }
