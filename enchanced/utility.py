
"""
Enhanced RAG Pipeline with BigQuery Function Calling and Gemini
Key improvements:
1. Dedicated BigQuery function declarations
2. Vector database using BigQuery
3. Enhanced context awareness
4. Robust intent recognition
5. Dynamic prompt refinement
"""

###############################################################################
# UTILITY FUNCTIONS
###############################################################################

from google.cloud import bigquery
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
from typing import List, Dict

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
