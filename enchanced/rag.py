###############################################################################
# RAG PIPELINE
###############################################################################

from google.cloud import bigquery
from typing import List, Dict,Union, Any
from vertexai.generative_models import (
    Tool,
    GenerativeModel
)
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
from vectordb import VectorDatabase
from config import list_datasets_func, list_tables_func, get_schema_func, execute_query_func

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
        
        # Initialize model with more conservative settings
        self.model = GenerativeModel(
            "gemini-1.5-pro",
            generation_config={
                "temperature": 0,
                "top_p": 0.8,
                "top_k": 40,
                "max_output_tokens": 1024,
            },
            tools=[self.tools]
        )

    def process_query(self, user_query: str) -> str:
        """Process user query through the RAG pipeline"""
        
        # 1. Intent Recognition & Context Enhancement
        relevant_context = self._get_relevant_context(user_query)
        
        # 2. Generate SQL with enhanced context
        sql_query = self._generate_sql(user_query, relevant_context)
        
        # 3. Execute and validate query
        results = self._execute_query(sql_query)
        
        # 4. Generate natural language response
        response = self._generate_response(user_query, sql_query, results)
        
        return response
        
# rag.py

    def _get_relevant_context(self, query: str) -> Dict:
        """Get relevant context using vector similarity"""
        try:
            similar_items = self.vector_db.similarity_search(query)
        except Exception as e:
            print(f"Warning: Vector similarity search failed: {str(e)}")
            similar_items = []
        
    # Extract table and column information
    try:
        tables_info = self._get_tables_info()
    except Exception as e:
        print(f"Warning: Failed to get tables info: {str(e)}")
        tables_info = {}
    
    return {
        "similar_contexts": similar_items,
        "tables_info": tables_info
    }

        
    def _get_tables_info(self) -> Dict:
        """Get information about available tables"""
        query = f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            description
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        """
        
        results = self.client.query(query).result()
        return {row['table_name']: dict(row) for row in results}
    
    
    def _execute_query(self, query: str) -> Union[List[Dict], str]:
        """Execute BigQuery SQL with error handling"""
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert to list of dicts
            rows = []
            for row in results:
                row_dict = {}
                for key in row.keys():
                    row_dict[key] = row[key]
                rows.append(row_dict)
            return rows
            
        except Exception as e:
            return f"ERROR: {str(e)}"
            
    def _generate_sql(
        self,
        user_query: str,
        context: Dict
    ) -> str:
        """Generate SQL with enhanced context using function calling"""
        
        try:
            chat = self.model.start_chat()
            
            # Combine context into a single prompt
            prompt = (
                "You are a SQL expert. Generate a BigQuery SQL query based on:\n"
                f"1) User question: {user_query}\n"
                f"2) Available schema: {context['tables_info']}\n"
                f"3) Relevant context: {context['similar_contexts']}\n"
                "Return ONLY the SQL query without explanation."
            )
            
            # Add retry logic and error handling
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = chat.send_message(prompt)
                    if hasattr(response, 'text') and response.text:
                        return response.text.strip()
                    else:
                        print(f"Empty response on attempt {attempt + 1}")
                        time.sleep(1)  # Add delay between retries
                except Exception as e:
                    print(f"Error on attempt {attempt + 1}: {str(e)}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(1)
                    
            return "SELECT 1"  # Fallback query if all retries fail
            
        except Exception as e:
            print(f"Error generating SQL: {str(e)}")
            return "SELECT 1"  # Fallback query
    
    def _generate_response(
        self,
        user_query: str,
        sql: str,
        results: List[Dict]
    ) -> str:
        """Generate natural language response using Gemini"""
        
        try:
            chat = self.model.start_chat()
            
            # Combine all context into a single prompt
            prompt = (
                "You are a helpful assistant that explains query results in natural language.\n"
                f"USER QUERY: {user_query}\n"
                f"SQL QUERY USED: {sql}\n"
                f"QUERY RESULTS: {str(results)}\n"
                "Please summarize these results in natural language."
            )
            
            # Add retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = chat.send_message(prompt)
                    if hasattr(response, 'text') and response.text:
                        return response.text.strip()
                    else:
                        print(f"Empty response on attempt {attempt + 1}")
                        time.sleep(1)
                except Exception as e:
                    print(f"Error on attempt {attempt + 1}: {str(e)}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(1)
                    
            return "Unable to generate response"  # Fallback response
            
        except Exception as e:
            print(f"Error generating response: {str(e)}")
            return "Unable to generate response"
    
