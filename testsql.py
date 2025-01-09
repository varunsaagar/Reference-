
import time
from google.cloud import bigquery
from google.api_core import exceptions
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool

# BigQuery configuration
BIGQUERY_PROJECT_ID = "vz-it-np-ienv-test-vegsdo-0"
BIGQUERY_DATASET_ID = "vegas_monitoring"

# Function declarations
list_datasets_func = FunctionDeclaration(
    name="list_datasets",
    description="Get a list of datasets that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {},
    },
)

list_tables_func = FunctionDeclaration(
    name="list_tables",
    description="List tables in a dataset that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {
            "dataset_id": {
                "type": "string",
                "description": "Dataset ID to fetch tables from.",
            }
        },
        "required": ["dataset_id"],
    },
)

get_table_func = FunctionDeclaration(
    name="get_table",
    description="Get information about a table, including the description, schema, and number of rows that will help answer the user's question. Always use the fully qualified dataset and table names.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "Fully qualified ID of the table to get information about",
            }
        },
        "required": ["table_id"],
    },
)

sql_query_func = FunctionDeclaration(
    name="sql_query",
    description="Get information from data in the database using SQL queries",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query on a single line that will help give quantitative answers to the user's question.",
            }
        },
        "required": ["query"],
    },
)

class DatabaseAnalyzer:
    def __init__(self):
        self.sql_query_tool = Tool(
            function_declarations=[
                list_datasets_func,
                list_tables_func,
                get_table_func,
                sql_query_func,
            ],
        )
        
        self.model = GenerativeModel(
            "gemini-1.5-pro",
            generation_config={"temperature": 0},
            tools=[self.sql_query_tool],
        )
        
        self.init_bigquery()
    
    def init_bigquery(self):
        """Initialize BigQuery client and check connection"""
        try:
            # Explicitly set project
            self.client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
            
            # Test the connection by trying to access the dataset
            dataset_ref = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}"
            self.client.get_dataset(dataset_ref)
            print("✅ Successfully connected to BigQuery")
            print(f"   Project: {BIGQUERY_PROJECT_ID}")
            print(f"   Dataset: {BIGQUERY_DATASET_ID}")
            
            # List available tables
            dataset = self.client.dataset(BIGQUERY_DATASET_ID)
            tables = list(self.client.list_tables(dataset))
            print(f"\nAvailable tables ({len(tables)}):")
            for table in tables:
                print(f"   • {table.table_id}")
                
        except exceptions.PermissionDenied as e:
            print("❌ Error: Permission denied. Please check your credentials and project access.")
            print(f"Detailed error: {str(e)}")
            raise
        except exceptions.NotFound:
            print("❌ Error: Dataset not found. Please check your project and dataset IDs.")
            raise
        except Exception as e:
            print(f"❌ Error connecting to BigQuery: {str(e)}")
            raise
        
    def process_query(self, prompt):
        """Process a natural language query and return the response"""
        chat = self.model.start_chat()
        
        enhanced_prompt = prompt + """
            Please analyze the data and provide a clear, concise summary of the findings.
            Focus on key metrics and insights that directly answer the question.
            """
        
        try:
            response = chat.send_message(enhanced_prompt)
            response = response.candidates[0].content.parts[0]
            
            function_calling_in_process = True
            results = []
            
            while function_calling_in_process:
                try:
                    if not hasattr(response, 'function_call'):
                        function_calling_in_process = False
                        continue
                        
                    params = {
                        key: value 
                        for key, value in response.function_call.args.items()
                    }
                    
                    # Execute query and get results
                    api_response = self._handle_bigquery_function(
                        response.function_call.name, 
                        params
                    )
                    
                    # Store results for summarization
                    results.append({
                        'function': response.function_call.name,
                        'params': params,
                        'response': api_response
                    })
                    
                    # Send results back to model for next step
                    response = chat.send_message(
                        Part.from_function_response(
                            name=response.function_call.name,
                            response={"content": api_response},
                        ),
                    )
                    response = response.candidates[0].content.parts[0]
                    
                except AttributeError:
                    function_calling_in_process = False
            
            # Generate final summary based on all results
            summary_prompt = f"""
                Based on the query results, provide a clear and concise summary:
                Query: {prompt}
                Results: {results}
                """
            
            final_response = chat.send_message(summary_prompt)
            return final_response.candidates[0].content.parts[0].text
            
        except Exception as e:
            return f"Error processing query: {str(e)}"
    
    def _handle_bigquery_function(self, function_name, params):
        """Execute BigQuery functions and return results"""
        try:
            if function_name == "sql_query":
                job_config = bigquery.QueryJobConfig(maximum_bytes_billed=100000000)
                cleaned_query = params["query"].replace("\\n", " ").replace("\n", "").replace("\\", "")
                
                # Execute query and wait for results
                query_job = self.client.query(cleaned_query, job_config=job_config)
                results = query_job.result()
                
                # Convert results to list of dictionaries
                return [dict(row) for row in results]
                
            elif function_name == "get_table":
                table = self.client.get_table(params["table_id"])
                return {
                    'description': table.description,
                    'schema': [field.name for field in table.schema],
                    'num_rows': table.num_rows
                }
                
            elif function_name == "list_tables":
                tables = self.client.list_tables(params["dataset_id"])
                return [table.table_id for table in tables]
                
            elif function_name == "list_datasets":
                return BIGQUERY_DATASET_ID
                
        except Exception as e:
            return f"Error executing BigQuery function {function_name}: {str(e)}"

def main():
    try:
        analyzer = DatabaseAnalyzer()
    except Exception as e:
        print(f"\nFailed to initialize database analyzer: {str(e)}")
        return

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
        response = analyzer.process_query(query)
        print("Response:", response)

if __name__ == "__main__":
    main()
