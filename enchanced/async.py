"""Calling Vegas api asynchronously for the limit set"""
import json
import time
import os
import pandas as pd
import asyncio
import aiohttp
from pydantic import BaseModel
import re

class LLMParameters(BaseModel):
    """
    The class represents LLM Parameters to be passed to vegas api.
    """
    top_k: float = 0.7
    top_p: float = 0.5
    temperature: float = 0.2
    repetition_penalty: float = 1.1
    max_new_tokens: int = 512
    min_new_tokens: int = 16   

def parse_response_string( vegas):
    try:
        vegas_res = vegas['prediction']
        vegas_res = vegas_res.strip()
        vegas_res = re.sub(r'\{\s+\{', '{{', vegas_res)
        vegas_res = re.sub(r'\}\s+\}', '}}', vegas_res)
        vegas_res = re.sub(' +', ' ', vegas_res)
        
        if(vegas_res.startswith('{{') and vegas_res.endswith('}}')):
            vegas_res = vegas_res[1:-1]
            vegas_res = vegas_res.strip()
        
        vegas_res = vegas_res.replace("```json", "").replace("```", "")    
        vegas_score = vegas_res
        # logger.info(f"341:: VEGAS Score  {vegas_score}")
    except Exception as ex:
        logger.error("********************************************** %s, vegas value: %s ********", ex, vegas)
        logger.critical(ex, exc_info=True)
        vegas_score = "Please try again later."
    # print(vegas_score)
    return vegas_score  
    
async def vegas_async(session, prompt, request_number):
    """
    asynchronously call Vegas url

    Returns
    -------
    None
    """
    print("********************* vegas_async **********************")
    params = LLMParameters().dict()
    c_usecase = 'LLM_EVALUATION_FRAMEWORK'
    context_id="BILLING_SLM_EVAL"
    context =     {
                    "{Question}": prompt,
                    } 
    parameters ={
                "temperature":0.9 ,
                "maxOutputTokens":4096,
                "topP": 1 ,
                "topK": 1}

    url = r"https://vegas-llm-batch.verizon.com/vegas/apps/batch/prompt/LLMInsight"
    # api_key = Config.get_required_config_var(r"jarvis_api_settings\api_key")
    payload_dict = {
            "useCase":'CALL_ANALYTICS_UI',
            "contextId": 'CALL_INTENT_TEST',
            "preSeed_injection_map": {
            "{INPUT}": prompt },
            "parameters": parameters
        }
    payload = json.dumps(payload_dict)
    headers = {
        'Content-Type': 'application/json',
    }
    logger.info('----------------------------------------'+payload+'`--------------------------------------')
    try:
        async with session.post(url, data=payload, headers=headers, ssl=False) as response_vegas:
            # Process the response here
            response_vegas = await response_vegas.json()
            print("Vegas Response:", response_vegas)
            response_vegas = parse_response_string(TEMP_MODELNAME.upper(), response_vegas)
            print("Vegas Final Response :", response_vegas)
            return {'prediction':response_vegas, 'index': request_number}
    except Exception as ex:
        logger.error("Vegas call failed with %s", ex)
        return {'prediction':{}, 'index': request_number}

 
def run_async_requests_gemini(df, max_requests_per_minute):
    """
    Creates task and run async batches

    Returns
    -------
    None
    """
    results = []
    print("********************* run_async_requests_gemini **********************")
    async def process_requests():
        print("********************* process_requests **********************")
        async with aiohttp.ClientSession() as session:
            tasks = []
            st = time.time()
            for row_index, row in df.iterrows():
                prompt = row['call_tr_with_prompt']
                task = asyncio.create_task(vegas_async(session, prompt, row_index))
                tasks.append(task)
                if ((row_index+1) % max_requests_per_minute == 0) or ((row_index+1) >= len(df)):
                    intermittent_results = await asyncio.gather(*tasks)
                    intermittent_results = sorted(intermittent_results, key=lambda x:(x["index"] is None, x["index"]))
                    results.extend([obj['prediction'] for obj in intermittent_results])
                    end_time = time.time()
                    logger.info("time taken for %d requests --- %f", len(tasks), end_time-st)
                    tasks = []
                    if (end_time-st) < 60 and ((row_index+1) < len(df)):
                        logger.info("Sleeeping for %f", 60-(end_time-st))
                        await asyncio.sleep(60-(end_time-st))
                    st = time.time()
            logger.info("time taken for %d requests --- %f", len(df), time.time()-st)
            return results
    return asyncio.run(process_requests())


if __name__ == '__main__':
    file_name= "dummy.csv"
    df = pd.read_csv(file_name)
    run_async_requests_gemini(df,10)



