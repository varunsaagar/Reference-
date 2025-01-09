Enter your question (or 'quit' to exit): Find the number of first time callers on Jun 1st who did not call before in the last 30 days

Processing query through RAG pipeline...

Error getting context: 400 Unable to submit request because it must have a text parameter. Add a text parameter and try again. Learn more: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/gemini
Error generating SQL: 'NoneType' object has no attribute 'name'

=== RESPONSE ===
The query failed to execute. This is because the provided SQL query is invalid. It only contains "SELECT 1", which doesn't specify any table or condition to query data from. To fix this, you need to write a valid SQL query that selects data from a specific table based on certain criteria.


###############################################################################
# RAG PIPELINE
###############################################################################

from google.cloud import bigquery
from typing import List, Dict, Union, Any
import json
import time
from vertexai.generative_models import (
    Tool,
    GenerativeModel,
    Part
)
from config import (
    BIGQUERY_PROJECT_ID, 
    BIGQUERY_DATASET_ID,
    list_datasets_func, 
    list_tables_func, 
    get_schema_func, 
    execute_query_func
)
from vectordb import VectorDatabase

class RAGPipeline:
    """Enhanced RAG Pipeline with BigQuery integration"""
    
    def __init__(self):
        self.vector_db = VectorDatabase()
        self.client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        
        # Initialize Gemini model with function calling
        self.tools = Tool(function_declarations=[
            list_datasets_func,
            list_tables_func, 
            get_schema_func,
            execute_query_func
        ])
        
        self.model = GenerativeModel(
            "gemini-1.5-pro",
            generation_config={"temperature": 0},
            tools=[self.tools]
        )

    def process_query(self, user_query: str) -> str:
        """Process user query through the RAG pipeline"""
        try:
            # 1. Get context using function calling
            relevant_context = self._get_relevant_context(user_query)
            
            # 2. Generate SQL with enhanced context
            sql_query = self._generate_sql(user_query, relevant_context)
            
            # 3. Execute query using function calling
            results = self._execute_query(sql_query)
            
            # 4. Generate response
            response = self._generate_response(user_query, sql_query, results)
            
            return response
            
        except Exception as e:
            return f"Error processing query: {str(e)}"

    def _get_relevant_context(self, query: str) -> Dict:
        """Get relevant context using vector similarity and function calling"""
        try:
            # Get similar items from vector store
            similar_items = self.vector_db.similarity_search(query)
            
            # Get table information using function calling
            chat = self.model.start_chat()
            
            # List available tables
            tables_response = chat.send_message(
                Part.from_function_response(
                    name="list_tables",
                    response={"dataset_id": BIGQUERY_DATASET_ID}
                )
            )
            
            # Get schema for each table
            tables_info = {}
            for table in tables_response.text.strip().split(','):
                table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table.strip()}"
                schema_response = chat.send_message(
                    Part.from_function_response(
                        name="get_schema",
                        response={"table_id": table_id}
                    )
                )
                tables_info[table.strip()] = json.loads(schema_response.text)
            
            return {
                "similar_contexts": similar_items,
                "tables_info": tables_info
            }
            
        except Exception as e:
            print(f"Error getting context: {str(e)}")
            return {"similar_contexts": [], "tables_info": {}}

    def _generate_sql(self, user_query: str, context: Dict) -> str:
        """Generate SQL using function calling"""
        try:
            chat = self.model.start_chat()
            
            prompt = (
                "Generate a BigQuery SQL query based on:\n"
                f"USER QUESTION: {user_query}\n"
                f"AVAILABLE SCHEMA: {json.dumps(context['tables_info'], indent=2)}\n"
                f"RELEVANT CONTEXT: {context['similar_contexts']}\n"
                "Return ONLY the SQL query."
            )
            
            response = chat.send_message(prompt)
            
            # Check if response contains function call
            if hasattr(response.candidates[0].content.parts[0], 'function_call'):
                function_call = response.candidates[0].content.parts[0].function_call
                if function_call.name == "execute_query":
                    return function_call.args['query']
            
            return response.text.strip()
            
        except Exception as e:
            print(f"Error generating SQL: {str(e)}")
            return "SELECT 1"

    def _execute_query(self, query: str) -> Union[List[Dict], str]:
        """Execute query using function calling"""
        try:
            chat = self.model.start_chat()
            response = chat.send_message(
                Part.from_function_response(
                    name="execute_query",
                    response={"query": query}
                )
            )
            
            results = json.loads(response.text)
            return results if isinstance(results, list) else []
            
        except Exception as e:
            return f"Error executing query: {str(e)}"

    def _generate_response(self, user_query: str, sql: str, results: List[Dict]) -> str:
        """Generate natural language response"""
        try:
            chat = self.model.start_chat()
            
            prompt = (
                "Explain these query results in natural language:\n"
                f"USER QUESTION: {user_query}\n"
                f"SQL QUERY: {sql}\n"
                f"RESULTS: {json.dumps(results, indent=2)}"
            )
            
            response = chat.send_message(prompt)
            return response.text.strip()
            
        except Exception as e:
            return f"Error generating response: {str(e)}"
