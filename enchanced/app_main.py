
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
