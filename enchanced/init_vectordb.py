# init_vectordb.py

from vectordb import VectorDatabase
from google.cloud import bigquery
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID

def init_vector_database():
    """Initialize and populate vector database with table/column metadata"""
    vector_db = VectorDatabase()
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    
    # Get all tables in dataset
    dataset_ref = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}"
    tables = list(client.list_tables(dataset_ref))
    
    # Store metadata for each table and column
    for table in tables:
        table_ref = client.get_table(table.reference)
        
        # Store table-level metadata
        table_text = f"Table: {table.table_id}, Description: {table_ref.description or 'No description'}"
        vector_db.store_embedding(
            text=table_text,
            metadata={"type": "table", "name": table.table_id}
        )
        
        # Store column-level metadata
        for field in table_ref.schema:
            col_text = (
                f"Table: {table.table_id}, Column: {field.name}, "
                f"Type: {field.field_type}, Description: {field.description or 'No description'}"
            )
            vector_db.store_embedding(
                text=col_text,
                metadata={
                    "type": "column",
                    "table": table.table_id,
                    "column": field.name
                }
            )
    
    print("✅ Vector database initialized and populated")

if __name__ == "__main__":
    init_vector_database()
