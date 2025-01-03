import subprocess
import json
import requests
from typing import List, Dict, Any
import time

class DatabaseAnalyzer:
    def __init__(self):
        self.project_id = "vz-it-np-ienv-test-vegsdo-0"
        self.dataset_id = "vegas_monitoring"
        self.llm_endpoint = "https://vegas-llm-test.ebiz.verizon.com/vegas/apps/prompt/LLMInsight"

    def execute_bq_command(self, command_type: str, params: Dict[str, Any] = None) -> str:
        """Execute BigQuery CLI commands and return results"""
        try:
            if command_type == "list_datasets":
                cmd = f"bq ls --project_id={self.project_id}"
            elif command_type == "list_tables":
                dataset_id = params.get("dataset_id", self.dataset_id)
                cmd = f"bq ls {self.project_id}:{dataset_id}"
            elif command_type == "get_table":
                table_id = params.get("table_id")
                cmd = f"bq show --format=json {table_id}"
            elif command_type == "sql_query":
                query = params.get("query", "").replace('"', '\\"')
                cmd = f'bq query --nouse_legacy_sql "{query}"'
            else:
                raise ValueError(f"Unknown command type: {command_type}")

            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"BQ Command failed: {result.stderr}")
                
            return result.stdout.strip()
            
        except Exception as e:
            print(f"Error executing BQ command: {str(e)}")
            return str(e)

    def call_llm_api(self, query: str) -> Dict:
        """Call the LLM API endpoint"""
        try:
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
            response = requests.post(self.llm_endpoint, json=payload, headers=headers)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            print(f"Error calling LLM API: {str(e)}")
            return {"error": str(e)}

    def process_query(self, prompt: str) -> str:
        """Process a natural language query using function calling pattern"""
        try:
            # First, get the SQL query from LLM
            llm_response = self.call_llm_api(prompt)
            
            # Simulate function calling pattern
            function_calls = []
            
            # List datasets
            datasets_result = self.execute_bq_command("list_datasets")
            function_calls.append({
                "name": "list_datasets",
                "params": {},
                "response": datasets_result
            })
            
            # List tables
            tables_result = self.execute_bq_command("list_tables", {"dataset_id": self.dataset_id})
            function_calls.append({
                "name": "list_tables",
                "params": {"dataset_id": self.dataset_id},
                "response": tables_result
            })
            
            # Extract SQL query from LLM response
            sql_query = llm_response.get("generated_sql", "")  # Adjust based on actual response format
            
            if sql_query:
                # Execute the SQL query
                query_result = self.execute_bq_command("sql_query", {"query": sql_query})
                function_calls.append({
                    "name": "sql_query",
                    "params": {"query": sql_query},
                    "response": query_result
                })
            
            # Format the response
            response = {
                "original_prompt": prompt,
                "function_calls": function_calls,
                "final_result": query_result if sql_query else "No SQL query generated"
            }
            
            return json.dumps(response, indent=2)
            
        except Exception as e:
            return f"Error processing query: {str(e)}"

def main():
    analyzer = DatabaseAnalyzer()
    
    # Sample queries
    sample_queries = [
        "What kind of information is in this database?",
        "What percentage of orders are returned?",
        "How is inventory distributed across our regional distribution centers?",
        "Do customers typically place more than one order?",
        "Which product categories have the highest profit margins?"
    ]
    
    print("\nSample queries you can try:", *sample_queries, sep="\n- ")
    
    while True:
        query = input("\nEnter your question (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
            
        print("\nProcessing query...\n")
        start_time = time.time()
        
        response = analyzer.process_query(query)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"\nResponse (processed in {processing_time:.2f} seconds):")
        print(response)
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    main()



Error calling LLM API: ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))

Response (processed in 14.63 seconds):
{
  "original_prompt": "how many rows in the table ",
  "function_calls": [
    {
      "name": "list_datasets",
      "params": {},
      "response": "datasetId                              \n ------------------------------------------------------------------ \n  Calculator                                                        \n  Test                                                              \n  agent_workflow_as_service                                         \n  agentic_services_demo                                             \n  agentic_services_demo_cal                                         \n  agentic_services_demo_calculator                                  \n  agentic_services_demo_fwa                                         \n  agentic_services_demo_fwa_multi_agent_endpoint_va                 \n  agentic_services_demo_fwa_multi_agent_endpoint_va1                \n  agentic_services_demo_fwa_single_agent                            \n  agentic_services_demo_fwa_single_agent_endpoint                   \n  agentic_services_demo_fwa_single_agent_endpoint_flash             \n  agentic_services_demo_fwa_single_agent_endpoint_flash_ritvik      \n  agentic_services_demo_fwa_single_agent_endpoint_flash_ritvikg     \n  agentic_services_demo_fwa_single_agent_endpoint_flash_ritvikg1    \n  agentic_services_demo_fwa_single_agent_endpoint_flash_ritvikgpro  \n  agentic_services_demo_fwa_single_agent_flash_ritvik               \n  agentic_services_demo_fwa_single_agent_saras                      \n  agentic_services_demo_fwa_single_agent_saras1                     \n  agentic_services_demo_fwa_single_agent_saras2                     \n  agentic_services_demo_fwa_single_agent_saras3                     \n  agentic_services_demo_fwa_va                                      \n  agentic_services_demo_fwa_va1                                     \n  agentic_services_demo_vcg                                         \n  agentic_services_demo_visible                                     \n  agentic_workflow_as_service                                       \n  calculator_demo                                                   \n  convo1                                                            \n  d3                                                                \n  final_hackathon_data                                              \n  multi_agentic_workflow                                            \n  r1                                                                \n  r4                                                                \n  s1                                                                \n  s3                                                                \n  sam1                                                              \n  samp1                                                             \n  sample1                                                           \n  sample_108                                                        \n  sample_dataset                                                    \n  sample_dataset1                                                   \n  sample_dataset_partioning                                         \n  sample_dataset_without_partioning                                 \n  ss1                                                               \n  t5                                                                \n  test                                                              \n  vegas_monitoring                                                  \n  vz_aid_vegs_gcp_logview                                           \n  vz_test_for_gemini_batch                                          \n  vz_vegas"
    },
    {
      "name": "list_tables",
      "params": {
        "dataset_id": "vegas_monitoring"
      },
      "response": "tableId          Type    Labels   Time Partitioning   Clustered Fields  \n ----------------------- ------- -------- ------------------- ------------------ \n  api_status_monitoring   TABLE"
    }
  ],
  "final_result": "No SQL query generated"
}

================================================================================

