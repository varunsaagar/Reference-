
import time
import sqlite3
from google.cloud import bigquery
from google.api_core import exceptions
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool


# "source_project_id":"vz-it-np-ienv-test-vegsdo-0",
# "source_dataset_id":"vegas_monitoring",
# "source_table_id":"api_status_monitoring",
# You can modify these constants for your specific dataset
BIGQUERY_PROJECT_ID = "vz-it-np-ienv-test-vegsdo-0"  # Replace with your project ID
BIGQUERY_DATASET_ID = "vegas_monitoring"
SQLITE_DB_PATH = "your_database.db"  # Replace with your SQLite database path
USE_BIGQUERY = True  # Set to False to use SQLite instead

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
    def __init__(self, use_bigquery=USE_BIGQUERY):
        self.use_bigquery = use_bigquery
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
        
        # Initialize database connection
        if self.use_bigquery:
            self.init_bigquery()
        else:
            self.init_sqlite()
    
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

    def init_sqlite(self):
        """Initialize SQLite connection and check database"""
        try:
            self.conn = sqlite3.connect(SQLITE_DB_PATH)
            self.cursor = self.conn.cursor()
            
            # Get list of tables
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = self.cursor.fetchall()
            
            print("✅ Successfully connected to SQLite database")
            print(f"   Database: {SQLITE_DB_PATH}")
            
            print(f"\nAvailable tables ({len(tables)}):")
            for table in tables:
                print(f"   • {table[0]}")
                
        except sqlite3.Error as e:
            print(f"❌ Error connecting to SQLite database: {str(e)}")
            raise
        
    def process_query(self, prompt):
        """Process a natural language query and return the response"""
        chat = self.model.start_chat()
        
        enhanced_prompt = prompt + """
            Please give a concise, high-level summary followed by detail in
            plain language about where the information in your response is
            coming from in the database. Only use information you learn
            from the database queries.
            """
        
        try:
            response = chat.send_message(enhanced_prompt)
            response = response.candidates[0].content.parts[0]
            
            function_calling_in_process = True
            while function_calling_in_process:
                try:
                    params = {}
                    for key, value in response.function_call.args.items():
                        params[key] = value
                        
                    # Handle different function calls based on database type
                    if self.use_bigquery:
                        api_response = self._handle_bigquery_function(response.function_call.name, params)
                    else:
                        api_response = self._handle_sqlite_function(response.function_call.name, params)
                    
                    print(f"Function called: {response.function_call.name}")
                    print(f"Parameters: {params}")
                    print(f"Response: {api_response}\n")
                    
                    response = chat.send_message(
                        Part.from_function_response(
                            name=response.function_call.name,
                            response={"content": api_response},
                        ),
                    )
                    response = response.candidates[0].content.parts[0]
                    
                except AttributeError:
                    function_calling_in_process = False
                    
            return response.text
            
        except Exception as e:
            return f"Error processing query: {str(e)}"
    
    def _handle_bigquery_function(self, function_name, params):
        """Handle BigQuery-specific function calls"""
        if function_name == "list_datasets":
            return BIGQUERY_DATASET_ID
            
        elif function_name == "list_tables":
            tables = self.client.list_tables(params["dataset_id"])
            return str([table.table_id for table in tables])
            
        elif function_name == "get_table":
            table = self.client.get_table(params["table_id"])
            table_info = table.to_api_repr()
            return str({
                'description': table_info.get('description', ''),
                'schema': [column['name'] for column in table_info['schema']['fields']]
            })
            
        elif function_name == "sql_query":
            job_config = bigquery.QueryJobConfig(maximum_bytes_billed=100000000)
            cleaned_query = params["query"].replace("\\n", " ").replace("\n", "").replace("\\", "")
            query_job = self.client.query(cleaned_query, job_config=job_config)
            results = query_job.result()
            return str([dict(row) for row in results])
    
    def _handle_sqlite_function(self, function_name, params):
        """Handle SQLite-specific function calls"""
        if function_name == "list_datasets":
            return "sqlite_database"
            
        elif function_name == "list_tables":
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return str([table[0] for table in self.cursor.fetchall()])
            
        elif function_name == "get_table":
            table_name = params["table_id"].split('.')[-1]  # Get just the table name
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            return str({
                'description': 'SQLite table',
                'schema': [column[1] for column in columns]
            })
            
        elif function_name == "sql_query":
            cleaned_query = params["query"].replace("\\n", " ").replace("\n", "").replace("\\", "")
            self.cursor.execute(cleaned_query)
            columns = [description[0] for description in self.cursor.description]
            results = self.cursor.fetchall()
            return str([dict(zip(columns, row)) for row in results])

def main():
    # Create analyzer instance - choose database type here
    try:
        analyzer = DatabaseAnalyzer(use_bigquery=USE_BIGQUERY)
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
