Error getting context: 'NoneType' object has no attribute 'args'
Error generating SQL: 'NoneType' object has no attribute 'args'
Resposne: The query returned a constant value of 1. This does not provide information about the number of first-time callers on June 1st who did not call in the last 30 days. The SQL query needs to be adjusted to calculate the desired result from the relevant data.
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
            
            # List tables using function calling
            list_tables_message = {
                "dataset_id": BIGQUERY_DATASET_ID
            }
            
            response = chat.send_message(
                "List the tables in the dataset",
                tools=[Tool(function_declarations=[list_tables_func])]
            )
            
            # Get schema for each table
            tables_info = {}
            if hasattr(response.candidates[0].content.parts[0], 'function_call'):
                tables = response.candidates[0].content.parts[0].function_call.args.get('tables', [])
                
                for table in tables:
                    schema_message = {
                        "table_id": f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table}"
                    }
                    
                    schema_response = chat.send_message(
                        f"Get schema for table {table}",
                        tools=[Tool(function_declarations=[get_schema_func])]
                    )
                    
                    if hasattr(schema_response.candidates[0].content.parts[0], 'function_call'):
                        tables_info[table] = schema_response.candidates[0].content.parts[0].function_call.args
            
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
                f"RELEVANT CONTEXT: {json.dumps(context['similar_contexts'], indent=2)}\n"
                "Return ONLY the SQL query."
            )
            
            response = chat.send_message(
                prompt,
                tools=[Tool(function_declarations=[execute_query_func])]
            )
            
            if hasattr(response.candidates[0].content.parts[0], 'function_call'):
                return response.candidates[0].content.parts[0].function_call.args.get('query', 'SELECT 1')
            
            return response.text.strip()
            
        except Exception as e:
            print(f"Error generating SQL: {str(e)}")
            return "SELECT 1"


    def _execute_query(self, query: str) -> Union[List[Dict], str]:
        """Execute query using function calling"""
        try:
            chat = self.model.start_chat()
            
            response = chat.send_message(
                f"Execute this SQL query: {query}",
                tools=[Tool(function_declarations=[execute_query_func])]
            )
            
            if hasattr(response.candidates[0].content.parts[0], 'function_call'):
                query_result = self.client.query(query).result()
                return [dict(row) for row in query_result]
            
            return []
            
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
