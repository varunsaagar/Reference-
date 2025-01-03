import subprocess
import json
import requests
import time
from typing import Optional, Union, List, Dict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseAnalyzer:
    def __init__(self):
        self.project_id = "vz-it-np-ienv-test-vegsdo-0"
        self.dataset_id = "vegas_monitoring"
        self.table_id = "api_status_monitoring"
        self.llm_endpoint = "https://vegas-llm-test.ebiz.verizon.com/vegas/apps/prompt/LLMInsight"
        
    def test_bq_connection(self) -> bool:
        """Test BigQuery connection by running a simple query"""
        try:
            test_query = f"""
                SELECT * 
                FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
                LIMIT 5
            """
            logger.info("Testing BigQuery connection...")
            result = self.execute_bq_command(test_query)
            
            if "error" in result:
                logger.error(f"BigQuery connection test failed: {result['error']}")
                return False
                
            logger.info("BigQuery connection test successful")
            logger.info("Sample data:")
            logger.info(json.dumps(result['data'], indent=2))
            return True
            
        except Exception as e:
            logger.error(f"BigQuery connection test failed: {str(e)}")
            return False

    def execute_bq_command(self, query: str) -> Dict:
        """Execute BigQuery command using bq CLI with improved error handling"""
        try:
            # Clean and format the query
            cleaned_query = query.replace('"', '\\"').replace('\n', ' ')
            cmd = f'bq query --nouse_legacy_sql "{cleaned_query}"'
            
            logger.info(f"Executing query: {cleaned_query}")
            
            # Execute the command
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Add timeout to prevent hanging
            stdout, stderr = process.communicate(timeout=60)
            
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
                if len(values) == len(headers):
                    data.append(dict(zip(headers, values)))
                
            return {"data": data}
            
        except subprocess.TimeoutExpired:
            logger.error("Query execution timed out")
            return {"error": "Query execution timed out"}
        except Exception as e:
            logger.error(f"Error executing BQ command: {str(e)}")
            return {"error": str(e)}

    def create_session_with_retry(self) -> requests.Session:
        """Create a session with improved retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,  # Increased from 3 to 5
            backoff_factor=2,  # Increased from 1 to 2
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"]  # Explicitly allow POST
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def call_llm_api(self, query: str, max_retries: int = 5) -> Dict:
        """Call the LLM API with improved retry mechanism"""
        session = self.create_session_with_retry()
        
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
        
        headers = {
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'
        }
        
        for attempt in range(max_retries):
            try:
                response = session.post(
                    self.llm_endpoint,
                    json=payload,
                    headers=headers,
                    timeout=60
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"All attempts failed: {str(e)}")
                    return {"error": str(e)}
                time.sleep(2 ** attempt)  # Exponential backoff

def main():
    analyzer = DatabaseAnalyzer()
    
    # First test BigQuery connection
    if not analyzer.test_bq_connection():
        print("Failed to connect to BigQuery. Exiting...")
        return
        
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
        
        # Execute direct BQ query for testing
        test_query = f"""
            SELECT *
            FROM `{analyzer.project_id}.{analyzer.dataset_id}.{analyzer.table_id}`
            LIMIT 5
        """
        response = analyzer.execute_bq_command(test_query)
        
        processing_time = time.time() - start_time
        print(f"\nResponse (processed in {processing_time:.2f} seconds):")
        print(json.dumps(response, indent=2))
        print("\n" + "="*80)

if __name__ == "__main__":
    main()


(text2sql) [domino@run-677775f203ca6841bc367eca-4kwk5 t2s]$ python3 bq_reader.py 
INFO:__main__:Testing BigQuery connection...
INFO:__main__:Executing query:                  SELECT *                  FROM `vz-it-np-ienv-test-vegsdo-0.vegas_monitoring.api_status_monitoring`                 LIMIT 5             
ERROR:__main__:BQ Query failed: /bin/sh: vz-it-np-ienv-test-vegsdo-0.vegas_monitoring.api_status_monitoring: command not found

ERROR:__main__:BigQuery connection test failed: /bin/sh: vz-it-np-ienv-test-vegsdo-0.vegas_monitoring.api_status_monitoring: command not found

Failed to connect to BigQuery. Exiting...
