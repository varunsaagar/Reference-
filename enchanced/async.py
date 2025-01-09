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
        prediction = prediction.replace("```json", "").replace("```", "")
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
        "contextId": "CALL_INTENT_TEST_Gemini_15_flash",
        "preSeed_injection_map": {
            "{INPUT}": prompt
        },
        "parameters": {
            "temperature": 0.9,
            "max_output_tokens": 8192,
            "top_p": 1,
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
        timeout = aiohttp.ClientTimeout(total=300) 
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
                
                elapsed = time.time() - batch_start_time
                if elapsed < 60 and batch_start + max_requests_per_minute < len(df):
                    wait_time = 60 - elapsed
                    print(f"Rate limiting: waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    
            return sorted(results, key=lambda x: x['index'])
            
    return asyncio.run(process_requests())

if __name__ == '__main__':
    file_name = "dummy.csv"
    df = pd.read_csv(file_name)
    
    if 'id' not in df.columns or 'prompt' not in df.columns:
        print("Error: CSV file must contain 'id' and 'prompt' columns")
        exit(1)
    
    print(f"Processing {len(df)} requests...")
    results = run_async_requests_vegas(df, 3)
    
    df['response'] = [r['prediction'] for r in results]
    
    df.to_csv(file_name, index=False)
    print(f"Processed {len(results)} requests and saved results to {file_name}")


curl -X POST "https://vegas-llm-batch.verizon.com/vegas/apps/batch/prompt/LLMInsight" \
-H "Content-Type: application/json" \
-d '{
  "useCase": "CALL_ANALYTICS_UI",
  "contextId": "CALL_INTENT_TEST",
  "preSeed_injection_map": {
    "{INPUT}": "prompt"
  },
  "parameters": {
    "temperature": 0.9,
    "maxOutputTokens": 8192,
    "topP": 1
  }
}'




{"prediction":"* **Write a story about a young woman who finds a magical book that grants her wishes.**\n* **Write a poem about the beauty of nature.**\n* **Write a screenplay for a short film about a group of friends who go on a road trip.**\n* **Write a business plan for a new product or service.**\n* **Write a grant proposal for a non-profit organization.**\n* **Write a speech about the importance of education.**\n* **Write a letter to your future self.**\n* **Write a song about your favorite band.**\n* **Write a review of a book, movie, or TV show.**\n* **Write a news article about a current event.**\n* **Write a blog post about your favorite hobby.**\n* **Write a short story about a time you overcame a challenge.**\n* **Write a poem about your dreams for the future.**\n* **Write a screenplay for a short film about a day in your life.**\n* **Write a business plan for your dream business.**\n* **Write a grant proposal for a project you are passionate about.**\n* **Write a speech about a cause you believe in.**\n* **Write a letter to someone who has inspired you.**\n* **Write a song about your favorite place in the world.**\n* **Write a review of a product or service you have used.**\n* **Write a news article about something that is important to you.**\n* **Write a blog post about a topic you are knowledgeable about.**","transactionMetadata":null,"responseMetadata":{"vegasTransactionId":"955926d9-9adc-450f-81a0-5f257dfcec71","timeTaken":2927,"httpStatusMessage":"OK","llmResponsePayload":[{"candidates":[{"content":{"role":"model","parts":[{"text":"*"}]}}],"usageMetadata":{},"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" **Write a story about a young woman who finds a magical book that grants her"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.08496094,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.12109375},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.06347656,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.07080078},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.07910156,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.07714844},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.28125,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.18164063}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" wishes.**\n* **Write a poem about the beauty of nature.**\n* **"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.05493164,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.057373047},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.040771484,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.068359375},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.06542969,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.040771484},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.21777344,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.140625}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":"Write a screenplay for a short film about a group of friends who go on a road trip.**\n* **Write a business plan for a new product or service.**\n"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.046142578,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.061035156},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.03564453,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.05419922},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.06347656,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.037841797},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.11425781,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.09033203}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":"* **Write a grant proposal for a non-profit organization.**\n* **Write a speech about the importance of education.**\n* **Write a letter to"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.048828125,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.07470703},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.024047852,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.049560547},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.080566406,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.046142578},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.06347656,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.06933594}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" your future self.**\n* **Write a song about your favorite band.**\n* **Write a review of a book, movie, or TV show.**\n* **Write a news article about a current event.**\n* **Write a"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.05102539,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.07470703},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.025512695,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.056640625},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.075683594,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.051757813},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.07373047,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.087402344}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" blog post about your favorite hobby.**\n* **Write a short story about a time you overcame a challenge.**\n* **Write a poem about your dreams for the future.**\n* **Write a screenplay for a short film about a day in"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.04736328,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.07080078},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.022949219,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.053466797},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.06933594,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.05102539},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.064453125,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.09423828}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" your life.**\n* **Write a business plan for your dream business.**\n* **Write a grant proposal for a project you are passionate about.**\n* **Write a speech about a cause you believe in.**\n* **Write a letter to someone who has inspired you.**\n* **Write a song about your"}]},"safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.04736328,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.06640625},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.022583008,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.053466797},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.07470703,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.049560547},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.06347656,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.083984375}]}],"modelVersion":"gemini-1.0-pro-001"},{"candidates":[{"content":{"role":"model","parts":[{"text":" favorite place in the world.**\n* **Write a review of a product or service you have used.**\n* **Write a news article about something that is important to you.**\n* **Write a blog post about a topic you are knowledgeable about.**"}]},"finishReason":"STOP","safetyRatings":[{"category":"HARM_CATEGORY_HATE_SPEECH","probability":"NEGLIGIBLE","probabilityScore":0.044677734,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.05834961},{"category":"HARM_CATEGORY_DANGEROUS_CONTENT","probability":"NEGLIGIBLE","probabilityScore":0.021240234,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.051757813},{"category":"HARM_CATEGORY_HARASSMENT","probability":"NEGLIGIBLE","probabilityScore":0.07080078,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.045410156},{"category":"HARM_CATEGORY_SEXUALLY_EXPLICIT","probability":"NEGLIGIBLE","probabilityScore":0.068359375,"severity":"HARM_SEVERITY_NEGLIGIBLE","severityScore":0.08642578}]}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":312,"totalTokenCount":314},"modelVersion":"gemini-1.0-pro-001"}],"httpStatusCode":200}}(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ 
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ 
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ 
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ python3 run_aync1.py Processing 5 requests...
Processing batch starting at index 0
Request 1 failed with status 400: {"vegasTransactionId":"04dd8183-105e-49a8-a9a8-03dd900a553f","errorCode":"BAD_REQUEST","messa
ge":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method
":"POST","timestamp":"2025-01-09T11:25:22.950217379"}
Request 2 failed with status 400: {"vegasTransactionId":"a0c82a64-b5d9-4b84-a15b-a0c1eb5962b4","errorCode":"BAD_REQUEST","messa
ge":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method
":"POST","timestamp":"2025-01-09T11:25:22.976956195"}
Request 0 failed with status 400: {"vegasTransactionId":"b0d2df5f-0209-4d30-9bd6-90c35a3d1090","errorCode":"BAD_REQUEST","messa
ge":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method
":"POST","timestamp":"2025-01-09T11:25:22.976432998"}
Rate limiting: waiting 59.69 seconds
Processing batch starting at index 3
Request 4 failed with status 400: {"vegasTransactionId":"0327afb9-0c8a-4a7d-a7ad-a38127117ff5","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:26:23.007896341"}
Request 3 failed with status 400: {"vegasTransactionId":"c2fd179a-8e3b-4786-ad3a-ad4673f555a2","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:26:23.043779713"}
Processed 5 requests and saved results to dummy.csv
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ python3 run_aync1.py 
Processing 5 requests...
Processing batch starting at index 0
Request 1 failed with status 400: {"vegasTransactionId":"711e4535-45c4-4de4-a93a-70dbc621c69e","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:26:44.069928761"}
Request 2 failed with status 400: {"vegasTransactionId":"e2515161-6516-4fdf-9600-13855ffdded2","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:26:44.072906658"}
Request 0 failed with status 400: {"vegasTransactionId":"9f8855ba-eead-426c-8f43-77844cfa1c76","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:26:44.099780068"}
Rate limiting: waiting 59.67 seconds
^CTraceback (most recent call last):
  File "/usr/lib64/python3.11/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib64/python3.11/asyncio/base_events.py", line 653, in run_until_complete
    return future.result()
           ^^^^^^^^^^^^^^^
  File "/mnt/async_test/run_aync1.py", line 110, in process_requests
    print(f"Rate limiting: waiting {wait_time:.2f} seconds")
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib64/python3.11/asyncio/tasks.py", line 639, in sleep
    return await future
           ^^^^^^^^^^^^
asyncio.exceptions.CancelledError

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/mnt/async_test/run_aync1.py", line 125, in <module>
    print(f"Processing {len(df)} requests...")
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/mnt/async_test/run_aync1.py", line 114, in run_async_requests_vegas
    
   ^^
  File "/usr/lib64/python3.11/asyncio/runners.py", line 190, in run
    return runner.run(main)
           ^^^^^^^^^^^^^^^^
  File "/usr/lib64/python3.11/asyncio/runners.py", line 123, in run
    raise KeyboardInterrupt()
KeyboardInterrupt

(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ ^C
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ ^C
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ ^C
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ ^C
(text2sql) [domino@run-677775f203ca6841bc367eca-68v5q async_test]$ python3 run_aync1.py 
Processing 5 requests...
Processing batch starting at index 0
Request 1 failed with status 400: {"vegasTransactionId":"81e2f75a-f2a0-4b3f-b0c7-6b7b9a7eaa2b","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:27:36.897385046"}
Request 2 failed with status 400: {"vegasTransactionId":"e808e3af-3e1e-41b0-9f95-a5c7c04171f8","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:27:36.899879074"}
Request 0 failed with status 400: {"vegasTransactionId":"6b53fd10-0644-402e-bf56-763a48bac7c7","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:27:36.94300056"}
Rate limiting: waiting 59.63 seconds
Processing batch starting at index 3
Request 3 failed with status 400: {"vegasTransactionId":"91bcaf62-e175-4c7f-8ba2-63ba053d2ce6","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:28:36.919211528"}
Request 4 failed with status 400: {"vegasTransactionId":"d8e676a6-222e-44b1-abbb-e683085151cb","errorCode":"BAD_REQUEST","message":"Invalid request payload.","statusCode":400,"statusName":"BAD_REQUEST","path":"/vegas/apps/batch/prompt/LLMInsight","method":"POST","timestamp":"2025-01-09T11:28:36.964684625"}
Processed 5 requests and saved results to dummy.csv
