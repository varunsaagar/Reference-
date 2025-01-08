
###############################################################################
# VECTOR DATABASE
###############################################################################

from google.cloud import bigquery
from typing import List, Dict,Union, Any
from vertexai.preview.language_models import TextEmbeddingModel
import time
from google.api_core import exceptions
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID

class VectorDatabase:
    """Manages embeddings and metadata in BigQuery"""
    
    def __init__(self):
        self.client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        self.embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-005")
        self._init_vector_store()
        
    def _init_vector_store(self):
        """Initialize BigQuery tables for vector store"""
        # Create embeddings table if not exists
        embedding_schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField(name="embedding",
                                field_type="FLOAT64",
                                mode="REPEATED"),
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
        
    # vectordb.py

    def similarity_search(self, query_text: str, k: int = 5) -> List[Dict]:
        """Find similar texts using cosine similarity in BigQuery"""
        try:
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
        except Exception as e:
            print(f"Warning: Similarity search failed: {str(e)}")
            return []  # Return empty list instead of failing
