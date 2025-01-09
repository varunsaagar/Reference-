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
        if not response or 'prediction' not in response:
            return "No prediction found"
            
        prediction = response['prediction']
        if not prediction:
            return "Empty prediction"
            
        prediction = prediction.strip()
        prediction = prediction.replace("```json", "").replace("```")
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
        "useCase": "LLM_EVALUATION_FRAMEWORK",
        "contextId": "BILLING_SLM_EVAL",
        "context": {
            "Question": prompt
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
            
            # Make sure to read the response data
            response_data = await response.json()
            return {'prediction': response_data, 'index': request_number}
            
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
        # Configure timeout and connector
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
    # Read the CSV file
    file_name = "dummy.csv"
    df = pd.read_csv(file_name)
    
    if 'id' not in df.columns or 'prompt' not in df.columns:
        print("Error: CSV file must contain 'id' and 'prompt' columns")
        exit(1)
    
    print(f"Processing {len(df)} requests...")
    results = run_async_requests_vegas(df, 3)  # Reduced batch size to 3 for better control
    
    # Add responses to the DataFrame
    df['response'] = [r['prediction'] for r in results]
    
    # Save back to the same CSV file
    df.to_csv(file_name, index=False)
    print(f"Processed {len(results)} requests and saved results to {file_name}")


Processing 5 requests...
Processing batch starting at index 0
Request 2 failed with status 400: {"vegasTransactionId":"02b7d70c-fe07-4110-8d87-bc0b30bc752b","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T10:53:27.393012663"}
Request 0 failed with status 400: {"vegasTransactionId":"c0ff7d7d-d31d-4b82-95a2-2d5062f24421","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T10:53:27.417467606"}
Request 1 failed with status 400: {"vegasTransactionId":"efcaf07d-4289-4acc-b48f-f392be0c9671","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T10:53:27.42059237"}
Rate limiting: waiting 59.67 seconds

