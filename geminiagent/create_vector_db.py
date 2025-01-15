import faiss
import numpy as np
import json
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel

# --- Configuration ---
PROJECT_ID = "your-project-id"  # Replace with your project ID
DATASET_ID = "your-dataset-id"  # Replace with your dataset ID
LOCATION = "us-central1"
EMBEDDING_MODEL_NAME = "textembedding-gecko@002"  # Vertex AI embedding model
INDEX_FILE = "call_center_embeddings.faiss"  # File to store the FAISS index

# --- Initialize Clients ---
bq_client = bigquery.Client(project=PROJECT_ID)
embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL_NAME)

def get_embeddings(texts):
    """Generates embeddings for a list of texts using the Vertex AI model."""
    embeddings = embedding_model.get_embeddings(texts)
    return [embedding.values for embedding in embeddings]

def create_faiss_index(embeddings):
    """Creates a FAISS index from a list of embeddings."""
    dimension = len(embeddings[0])
    index = faiss.IndexFlatL2(dimension)  # Using L2 distance for similarity
    index.add(np.array(embeddings, dtype=np.float32))
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
            "table_description": table.description or "",
            "columns": [],
        }

        for field in table.schema:
            column_info = {
                "column_name": field.name,
                "column_type": field.field_type,
                "column_description": field.description or "",
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
