import subprocess
import json
import requests
import time
from typing import Optional, Union, List, Dict
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
            # Correct bq command format
            test_query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}` LIMIT 5"
            cmd = ['bq', 'query', '--nouse_legacy_sql', test_query]
            
            logger.info("Testing BigQuery connection...")
            logger.info(f"Executing command: {' '.join(cmd)}")
            
            # Execute command without shell=True for better security
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(timeout=60)
            
            if process.returncode != 0:
                logger.error(f"BigQuery connection test failed: {stderr}")
                return False
            
            logger.info("BigQuery connection test successful")
            logger.info("Sample data:")
            logger.info(stdout)
            return True
            
        except Exception as e:
            logger.error(f"BigQuery connection test failed: {str(e)}")
            return False

    def execute_bq_command(self, query: str) -> Dict:
        """Execute BigQuery command using bq CLI with improved error handling"""
        try:
            # Use list format for command to avoid shell injection
            cmd = ['bq', 'query', '--nouse_legacy_sql', query]
            
            logger.info(f"Executing query: {query}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(timeout=60)
            
            if process.returncode != 0:
                logger.error(f"BQ Query failed: {stderr}")
                return {"error": stderr}
            
            # Parse the output into a structured format
            rows = stdout.strip().split('\n')
            if len(rows) < 2:
                return {"data": []}
            
            headers = rows[0].split()  # Split on whitespace instead of comma
            data = []
            for row in rows[1:]:
                values = row.split()
                if len(values) == len(headers):
                    data.append(dict(zip(headers, values)))
            
            return {"data": data}
            
        except subprocess.TimeoutExpired:
            logger.error("Query execution timed out")
            return {"error": "Query execution timed out"}
        except Exception as e:
            logger.error(f"Error executing BQ command: {str(e)}")
            return {"error": str(e)}

    def call_llm_api(self, query: str, max_retries: int = 5) -> Dict:
        """Call the LLM API with improved retry mechanism"""
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
                response = requests.post(
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
                time.sleep(2 ** attempt)

def main():
    analyzer = DatabaseAnalyzer()
    
    # First test BigQuery connection
    print("\nTesting BigQuery connection...")
    if not analyzer.test_bq_connection():
        print("Failed to connect to BigQuery. Please check your credentials and permissions.")
        return
    
    print("\nBigQuery connection successful!")
    
    # Test direct query execution
    print("\nTesting direct query execution...")
    test_query = f"SELECT * FROM `{analyzer.project_id}.{analyzer.dataset_id}.{analyzer.table_id}` LIMIT 5"
    result = analyzer.execute_bq_command(test_query)
    print("\nDirect query result:")
    print(json.dumps(result, indent=2))
    
    # Only proceed with LLM if BigQuery is working
    print("\nWelcome to Database Analyzer!")
    print("Type 'quit' to exit or 'help' for sample queries")
    
    sample_queries = [
        "Show me the total number of rows in api_status_monitoring table",
        "What are the column names in api_status_monitoring table?",
        "Show me the latest 5 records from api_status_monitoring"
    ]
    
    while True:
        command = input("\nEnter your question (or 'quit'/'help'): ").strip()
        
        if command.lower() == 'quit':
            break
        elif command.lower() == 'help':
            print("\nSample queries:")
            for i, query in enumerate(sample_queries, 1):
                print(f"{i}. {query}")
            continue
        
        print("\nExecuting query directly first...")
        direct_result = analyzer.execute_bq_command(
            f"SELECT * FROM `{analyzer.project_id}.{analyzer.dataset_id}.{analyzer.table_id}` LIMIT 5"
        )
        print("\nDirect query result:")
        print(json.dumps(direct_result, indent=2))

if __name__ == "__main__":
    main()
