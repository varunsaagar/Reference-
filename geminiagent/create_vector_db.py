import faiss
import numpy as np
import json
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel
from google.api_core.future.polling import DEFAULT_POLLING
import time
from tenacity import retry, stop_after_attempt, wait_exponential

# --- Configuration ---
PROJECT_ID = "your-project-id"  # Replace with your project ID
DATASET_ID = "your-dataset-id"  # Replace with your dataset ID
LOCATION = "us-central1"
EMBEDDING_MODEL_NAME = "textembedding-gecko@002"  # Vertex AI embedding model
INDEX_FILE = "call_center_embeddings.faiss"  # File to store the FAISS index
BATCH_SIZE = 250  # Maximum number of instances per batch (limit is 250)

# --- Initialize Clients ---
bq_client = bigquery.Client(project=PROJECT_ID)
embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL_NAME)

# Increase the default polling timeout to 3600 seconds (1 hour)
DEFAULT_POLLING._timeout = 3600

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def get_embeddings_with_retry(texts_batch):
    """Generates embeddings for a batch of texts with retry logic."""
    try:
        response = embedding_model.get_embeddings(texts_batch)
        return [embedding.values for embedding in response]
    except Exception as e:
        print(f"Error in embedding generation: {e}")
        raise

def get_embeddings(texts):
    """Generates embeddings for a list of texts using the Vertex AI model with improved error handling."""
    embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        # Filter out empty strings from the batch
        batch = [text for text in batch if text]
        if not batch:
            continue
        
        try:
            # Add delay between batches to prevent rate limiting
            if i > 0:
                time.sleep(1)
            
            batch_embeddings = get_embeddings_with_retry(batch)
            embeddings.extend(batch_embeddings)
            print(f"Successfully processed batch {i//BATCH_SIZE + 1}")
        except Exception as e:
            print(f"Failed to process batch starting at index {i}: {e}")
            # Continue with next batch instead of failing completely
            continue
    
    return embeddings

def create_faiss_index(embeddings):
    """Creates a FAISS index from a list of embeddings using cosine similarity."""
    dimension = len(embeddings[0])
    index = faiss.IndexFlatIP(dimension)  # Using Inner Product for cosine similarity
    # Normalize embeddings for cosine similarity
    embeddings_array = np.array(embeddings, dtype=np.float32) # added this line to fix the error
    faiss.normalize_L2(embeddings_array) # add the embeddings array here
    index.add(embeddings_array)
    return index

def main():
    """
    Creates a vector database (FAISS index) of table schemas and distinct values.
    """
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    table_data = []

    # Fetch table and column information
    tables = bq_client.list_tables(dataset_ref)
    for table in tables:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}"
        table = bq_client.get_table(table_ref)

        table_info = {
            "table_name": table.table_id,
            "table_description": table.description or "No description available",
            "columns": [],
        }

        for field in table.schema:
            column_info = {
                "column_name": field.name,
                "column_type": field.field_type,
                "column_description": field.description or "No description available",
                "distinct_values": [],
            }

            # Get distinct values (limit to top 10 for this example)
            if field.field_type in ["STRING", "ENUM"]:
                try:
                    query = f"SELECT DISTINCT {field.name} FROM `{table_ref}` WHERE {field.name} IS NOT NULL LIMIT 10"
                    query_job = bq_client.query(query)
                    distinct_values = [row[0] for row in query_job.result()]
                    column_info["distinct_values"] = distinct_values
                except Exception as e:
                    print(f"Error getting distinct values for {table_ref}.{field.name}: {e}")

            table_info["columns"].append(column_info)
        table_data.append(table_info)

    # Generate embeddings
    texts_to_embed = []
    index_data = []  # To keep track of what each embedding represents

    for table_info in table_data:
        texts_to_embed.append(table_info["table_name"])
        index_data.append({"type": "table", "table": table_info["table_name"]})

        texts_to_embed.append(table_info["table_description"])
        index_data.append({"type": "table_description", "table": table_info["table_name"]})

        for column_info in table_info["columns"]:
            texts_to_embed.append(column_info["column_name"])
            index_data.append(
                {
                    "type": "column",
                    "table": table_info["table_name"],
                    "column": column_info["column_name"],
                }
            )

            texts_to_embed.append(column_info["column_description"])
            index_data.append(
                {
                    "type": "column_description",
                    "table": table_info["table_name"],
                    "column": column_info["column_name"],
                }
            )

            for value in column_info["distinct_values"]:
                texts_to_embed.append(value)
                index_data.append(
                    {
                        "type": "value",
                        "table": table_info["table_name"],
                        "column": column_info["column_name"],
                        "value": value,
                    }
                )

    embeddings = get_embeddings(texts_to_embed)

    # Create and save FAISS index
    index = create_faiss_index(embeddings)
    faiss.write_index(index, INDEX_FILE)

    # Save index_data (mapping of embeddings to table/column/value)
    with open("index_data.json", "w") as f:
        json.dump(index_data, f)

    print(f"Vector database created and saved to {INDEX_FILE} and index_data.json")


if __name__ == "__main__":
    main()
