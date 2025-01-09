
###############################################################################
# DEMO APPLICATION
###############################################################################

from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
from rag import RAGPipeline

def main():
    """
    Demo application showing the complete RAG pipeline with BigQuery
    and function calling.
    """
    pipeline = RAGPipeline()
    response = pipeline.process_query("Find the number of first time callers on Jun 1st who did not call before in the last 30 days")
    print("Resposne:",response)

if __name__ == "__main__":
    main()
