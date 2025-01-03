import subprocess
import json
import requests
import time
from typing import Optional, Union, List, Dict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseAnalyzer:
    def __init__(self):
        self.project_id = "vz-it-np-ienv-test-vegsdo-0"
        self.dataset_id = "vegas_monitoring"
        self.llm_endpoint = "https://vegas-llm-test.ebiz.verizon.com/vegas/apps/prompt/LLMInsight"
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def execute_bq_command(self, query: str) -> Dict:
        """Execute BigQuery command using bq CLI"""
        try:
            cmd = f'bq query --nouse_legacy_sql "{query}"'
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                logger.error(f"BQ Query failed: {stderr}")
                return {"error": stderr}
            
            # Parse the output into a structured format
            rows = stdout.strip().split('\n')
            if len(rows) < 2:
                return {"data": []}
                
            headers = rows[0].split(',')
            data = []
            for row in rows[1:]:
                values = row.split(',')
                data.append(dict(zip(headers, values)))
                
            return {"data": data}
            
        except Exception as e:
            logger.error(f"Error executing BQ command: {str(e)}")
            return {"error": str(e)}

    def call_llm_api(self, query: str, max_retries: int = 3) -> Dict:
        """Call the LLM API with retry mechanism"""
        payload = {
            "useCase": "text2sql",
            "contextId": "zero_shot_context",
            "preSeed_injection_map": {
                "{Query}": query
            },
            "parameters": {
                "temperature": 0.9,
                "maxOutputTokens": 2048,
                "topP": 1
            }
        }
        
        headers = {'Content-Type': 'application/json'}
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.llm_endpoint,
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"All attempts failed: {str(e)}")
                    return {"error": str(e)}
                time.sleep(2 ** attempt)  # Exponential backoff

    def process_query(self, query: str) -> str:
        """Process a natural language query and return the response"""
        try:
            # Get SQL query from LLM
            logger.info(f"Processing query: {query}")
            llm_response = self.call_llm_api(query)
            
            if "error" in llm_response:
                return f"LLM API Error: {llm_response['error']}"
            
            # Extract SQL query from LLM response
            sql_query = llm_response.get('generated_sql', '')
            if not sql_query:
                return "No SQL query was generated"
            
            # Execute the query
            logger.info(f"Executing SQL query: {sql_query}")
            results = self.execute_bq_command(sql_query)
            
            if "error" in results:
                return f"Query execution error: {results['error']}"
                
            return json.dumps(results['data'], indent=2)
            
        except Exception as e:
            logger.error(f"Error in process_query: {str(e)}")
            return f"Error processing query: {str(e)}"

def main():
    analyzer = DatabaseAnalyzer()
    
    print("\nWelcome to Database Analyzer!")
    print("Type 'quit' to exit or 'help' for sample queries")
    
    sample_queries = [
        "Show me the total number of rows in api_status_monitoring table",
        "What are the column names in api_status_monitoring table?",
        "Show me the latest 5 records from api_status_monitoring"
    ]
    
    while True:
        command = input("\nEnter your question: ").strip().lower()
        
        if command == 'quit':
            break
        elif command == 'help':
            print("\nSample queries:")
            for i, query in enumerate(sample_queries, 1):
                print(f"{i}. {query}")
            continue
        
        print("\nProcessing query...\n")
        start_time = time.time()
        
        response = analyzer.process_query(command)
        
        processing_time = time.time() - start_time
        print(f"\nResponse (processed in {processing_time:.2f} seconds):")
        print(response)
        print("\n" + "="*80)

if __name__ == "__main__":
    main()
