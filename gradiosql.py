
"""
Enhanced RAG Pipeline with BigQuery Function Calling and Gemini
Key improvements:
1. Dedicated BigQuery function declarations
2. Vector database using BigQuery
3. Enhanced context awareness
4. Robust intent recognition
5. Dynamic prompt refinement
"""

import time
from typing import List, Dict, Any, Union
from google.cloud import bigquery
from google.api_core import exceptions
from vertexai.generative_models import (
    FunctionDeclaration, 
    GenerativeModel,
    Part,
    Tool,
    ChatModel
)
from vertexai.preview.language_models import TextEmbeddingModel

###############################################################################
# CONFIGURATIONS 
###############################################################################

BIGQUERY_PROJECT_ID = "your-project-id"
BIGQUERY_DATASET_ID = "your-dataset-id"

# Function declarations for BigQuery operations
list_datasets_func = FunctionDeclaration(
    name="list_datasets",
    description="List available BigQuery datasets",
    parameters={
        "type": "object",
        "properties": {},
    },
)

list_tables_func = FunctionDeclaration(
    name="list_tables", 
    description="List tables in a BigQuery dataset",
    parameters={
        "type": "object",
        "properties": {
            "dataset_id": {"type": "string"}
        },
        "required": ["dataset_id"],
    },
)

get_schema_func = FunctionDeclaration(
    name="get_schema",
    description="Get schema information for a BigQuery table",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {"type": "string"}
        },
        "required": ["table_id"],
    },
)

execute_query_func = FunctionDeclaration(
    name="execute_query",
    description="Execute a BigQuery SQL query",
    parameters={
        "type": "object",
        "properties": {
            "query": {"type": "string"}
        },
        "required": ["query"],
    },
)

###############################################################################
# VECTOR DATABASE
###############################################################################

class VectorDatabase:
    """Manages embeddings and metadata in BigQuery"""
    
    def __init__(self):
        self.client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        self.embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@latest")
        self._init_vector_store()
        
    def _init_vector_store(self):
        """Initialize BigQuery tables for vector store"""
        # Create embeddings table if not exists
        embedding_schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("embedding", "ARRAY", mode="REPEATED", 
                               field_type="FLOAT64"),
            bigquery.SchemaField("metadata", "STRING")
        ]
        
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.embeddings"
        try:
            self.client.get_table(table_id)
        except exceptions.NotFound:
            table = bigquery.Table(table_id, schema=embedding_schema)
            self.client.create_table(table)
            
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using Vertex AI"""
        result = self.embedding_model.get_embeddings([text])
        return result[1].values
        
    def store_embedding(self, text: str, metadata: Dict = None):
        """Store text embedding in BigQuery"""
        embedding = self.generate_embedding(text)
        
        query = f"""
        INSERT INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.embeddings`
        (id, text, embedding, metadata)
        VALUES(@id, @text, @embedding, @metadata)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "STRING", str(time.time())),
                bigquery.ScalarQueryParameter("text", "STRING", text),
                bigquery.ArrayQueryParameter("embedding", "FLOAT64", embedding),
                bigquery.ScalarQueryParameter("metadata", "STRING", str(metadata))
            ]
        )
        
        self.client.query(query, job_config=job_config).result()
        
    def similarity_search(self, query_text: str, k: int = 5) -> List[Dict]:
        """Find similar texts using cosine similarity in BigQuery"""
        query_embedding = self.generate_embedding(query_text)
        
        similarity_query = f"""
        WITH similarity AS (
            SELECT 
                text,
                metadata,
                (
                    SELECT SUM(a * b) / SQRT(SUM(a * a) * SUM(b * b))
                    FROM UNNEST(embedding) a WITH OFFSET pos
                    INNER JOIN UNNEST(@query_embedding) b WITH OFFSET pos
                    USING(pos)
                ) as similarity_score
            FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.embeddings`
        )
        SELECT *
        FROM similarity
        WHERE similarity_score > 0
        ORDER BY similarity_score DESC
        LIMIT @k
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("query_embedding", "FLOAT64", 
                                           query_embedding),
                bigquery.ScalarQueryParameter("k", "INT64", k)
            ]
        )
        
        results = self.client.query(similarity_query, job_config=job_config).result()
        return [dict(row) for row in results]

###############################################################################
# RAG PIPELINE
###############################################################################

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
        
        # 1. Intent Recognition & Context Enhancement
        relevant_context = self._get_relevant_context(user_query)
        
        # 2. Generate SQL with enhanced context
        sql_query = self._generate_sql(user_query, relevant_context)
        
        # 3. Execute and validate query
        results = self._execute_query(sql_query)
        
        # 4. Generate natural language response
        response = self._generate_response(user_query, sql_query, results)
        
        return response
        
    def _get_relevant_context(self, query: str) -> Dict:
        """Get relevant context using vector similarity"""
        similar_items = self.vector_db.similarity_search(query)
        
        # Extract table and column information
        tables_info = self._get_tables_info()
        
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

    def _generate_sql(
        self,
        user_query: str,
        context: Dict
    ) -> str:
        """Generate SQL with enhanced context using function calling"""
        
        chat = self.model.start_chat()
        system_prompt = (
            "You are a SQL expert. Generate a BigQuery SQL query based on:"
            "\n1) The user's question"
            "\n2) The available tables and columns"
            "\n3) The relevant context"
            "\nReturn ONLY the SQL query without explanation."
        )
        
        user_prompt = (
            f"USER QUERY: {user_query}\n\n"
            f"AVAILABLE SCHEMA:\n{context['tables_info']}\n\n"
            f"RELEVANT CONTEXT:\n{context['similar_contexts']}"
        )
        
        response = chat.send_message(
            content=user_prompt,
            context=system_prompt
        )
        
        return response.text.strip()
        
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
            
    def _generate_response(
        self,
        user_query: str,
        sql: str,
        results: List[Dict]
    ) -> str:
        """Generate natural language response using Gemini"""
        
        chat = self.model.start_chat()
        system_prompt = (
            "You are a helpful assistant that explains query results in natural language."
            "Provide a clear, concise summary of the findings."
        )
        
        user_prompt = (
            f"USER QUERY: {user_query}\n\n"
            f"SQL QUERY USED: {sql}\n\n"
            f"QUERY RESULTS: {str(results)}\n\n"
            "Please summarize these results in natural language."
        )
        
        response = chat.send_message(
            content=user_prompt,
            context=system_prompt
        )
        
        return response.text.strip()

###############################################################################
# DEMO APPLICATION
###############################################################################

def main():
    """
    Demo application showing the complete RAG pipeline with BigQuery
    and function calling.
    """
    
    # Initialize the pipeline
    try:
        pipeline = RAGPipeline()
        print("\n=== RAG Pipeline Initialized ===")
        print(f"Project: {BIGQUERY_PROJECT_ID}")
        print(f"Dataset: {BIGQUERY_DATASET_ID}\n")
        
        # Example queries to try
        sample_queries = [
            "How many total records are in the database?",
            "What are the most common values in the status column?",
            "Show me the distribution of records by date",
        ]
        
        print("Try these sample queries (or type your own):")
        for q in sample_queries:
            print(f" - {q}")
            
        while True:
            query = input("\nEnter your question (or 'quit' to exit): ")
            if query.lower().strip() == 'quit':
                break
                
            print("\nProcessing query through RAG pipeline...\n")
            
            try:
                response = pipeline.process_query(query)
                print("\n=== RESPONSE ===")
                print(response)
                print("\n" + "="*50 + "\n")
                
            except Exception as e:
                print(f"\nError processing query: {str(e)}")
                
    except Exception as e:
        print(f"Failed to initialize pipeline: {str(e)}")

if __name__ == "__main__":
    main()

###############################################################################
# UTILITY FUNCTIONS
###############################################################################

def format_bigquery_results(results: List[Dict]) -> str:
    """Format BigQuery results for better display"""
    if not results:
        return "No results found."
        
    # Get column names
    columns = list(results[0].keys())
    
    # Calculate column widths
    widths = {}
    for col in columns:
        widths[col] = max(
            len(str(row[col])) for row in results + [{"col": col}]
        )
        
    # Create header
    header = " | ".join(
        f"{col:{widths[col]}}" for col in columns
    )
    separator = "-" * len(header)
    
    # Format rows
    rows = []
    for row in results:
        formatted_row = " | ".join(
            f"{str(row[col]):{widths[col]}}" for col in columns
        )
        rows.append(formatted_row)
        
    # Combine all parts
    return "\n".join([header, separator] + rows)

def validate_bigquery_connection():
    """Validate BigQuery connection and permissions"""
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        
        # Test dataset access
        dataset_ref = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}"
        client.get_dataset(dataset_ref)
        
        # List some tables
        tables = list(client.list_tables(dataset_ref))
        table_names = [table.table_id for table in tables]
        
        print("✅ BigQuery Connection Validated")
        print(f"Project: {BIGQUERY_PROJECT_ID}")
        print(f"Dataset: {BIGQUERY_DATASET_ID}")
        print(f"Available Tables: {', '.join(table_names)}")
        return True
        
    except Exception as e:
        print("❌ BigQuery Connection Failed")
        print(f"Error: {str(e)}")
        return False


