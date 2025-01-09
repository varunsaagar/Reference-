

# data_access.py
from google.cloud import bigquery
from google.api_core import exceptions

class BigQueryManager:
    def __init__(self, project_id, dataset_id, table_id):
        # Hardcode project, dataset, and table IDs
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        # Use default credentials
        self.client = bigquery.Client(project=self.project_id)

    def get_table_schema(self):
        """Retrieves the schema of the specified BigQuery table."""
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            table = self.client.get_table(table_ref)
            schema_info = []
            for field in table.schema:
                schema_info.append(
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "description": field.description,
                    }
                )
            return schema_info
        except exceptions.NotFound:
            print(f"Table {table_ref} not found")
            return None
        except exceptions.Forbidden:
            print(
                f"Permission denied to access table {table_ref}. "
                "Make sure you have the necessary permissions."
            )
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def execute_query(self, query):
        """Executes a SQL query against BigQuery and returns the results."""
        try:
            query_job = self.client.query(query)
            results = query_job.result()  # Waits for the query to finish
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    def get_distinct_values(self, column_name, limit=10):
        """Retrieves a sample of distinct values from a specified column."""
        try:
            query = f"""
                SELECT DISTINCT {column_name}
                FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
                WHERE {column_name} IS NOT NULL
                LIMIT {limit}
            """
            query_job = self.client.query(query)
            results = query_job.result()
            return [row[0] for row in results]
        except Exception as e:
            print(f"Error getting distinct values for {column_name}: {e}")
            return None
