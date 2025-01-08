from vertexai.generative_models import (
    FunctionDeclaration
)


###############################################################################
# CONFIGURATIONS 
###############################################################################

BIGQUERY_PROJECT_ID = "vz-it-np-ienv-test-vegsdo-0"
BIGQUERY_DATASET_ID = "vegas_monitoring"

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
