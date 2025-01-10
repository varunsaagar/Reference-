from google.cloud import bigquery
from google.api_core import exceptions


class BigQueryManager:
    def __init__(self, project_id, dataset_id):
        # Only project and dataset in init
        self.project_id = project_id
        self.dataset_id = dataset_id
        # self.table_id = table_id  # Remove table_id from here

        # Use default credentials
        self.client = bigquery.Client(project=self.project_id)

    def get_table_schema(self, table_id):  # Add table_id parameter
        """Retrieves the schema of the specified BigQuery table."""
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"  # Use table_id here
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

    def get_table_descriptions(self):
        """
        Retrieves a dictionary of table names and their descriptions for all tables in the dataset.
        """
        try:
            tables = self.client.list_tables(self.dataset_id)
            table_descriptions = {}
            for table in tables:
                full_table_ref = f"{self.project_id}.{self.dataset_id}.{table.table_id}"
                table_obj = self.client.get_table(full_table_ref)
                table_descriptions[table.table_id] = table_obj.description if table_obj.description else ""
                # print(f"Table: {table.table_id}, Description: {table_obj.description}") #Uncomment for debugging
            return table_descriptions
        except Exception as e:
            print(f"Error getting table descriptions: {e}")
            return {}
        
    def get_distinct_values(self, column_name, table_id, limit=10):  # Add table_id parameter
        """Retrieves a sample of distinct values from a specified column."""
        try:
            query = f"""
                SELECT DISTINCT {column_name}
                FROM `{self.project_id}.{self.dataset_id}.{table_id}`
                WHERE {column_name} IS NOT NULL
                LIMIT {limit}
            """
            query_job = self.client.query(query)
            results = query_job.result()
            return [row[0] for row in results]
        except Exception as e:
            print(f"Error getting distinct values for {column_name}: {e}")
            return None
