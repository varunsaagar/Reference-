
# main.py
from gemini_agent import GeminiAgent
from utils import format_user_query

# Hardcoded BigQuery details
PROJECT_ID = "vz-it-np-ienv-test-vegsdo-0"  # Replace with your actual project ID
DATASET_ID = "vegas_monitoring"  # Replace with your actual dataset ID
# TABLE_ID = "icm_summary_fact_exp"  # Replace with your actual table ID
LOCATION = "us-central1"  # Or your preferred location


def main():
    agent = GeminiAgent(PROJECT_ID, DATASET_ID, LOCATION)

    # Hardcoded user query
    # user_query = "What was the average call duration for technical support calls yesterday?"
    # user_query = "How many calls were abandoned yesterday?"
    # user_query = "How many calls were received for billing last month?"

    # user_query = "List all columns in the table"
    # user_query = "List all columns in the table with its description"

    # user_query = "Find the number of first time callers on Jun 1st who did not call before in the last 30 days"


    # user_query = "What was the average call handling time for billing inquiries in the last week, broken down by agent?"
    # user_query = "What was the average call duration for billing inquiries last week?"

    # user_query = "  Find the number of abandoned calls based on department for the latest available information but exclude prepay calls. I also need to know average, min and max time before the call is abandoned"
    # user_query = "What is the average handle time for calls handled by each agent in the last week"
    # user_query = "What is the average handle time for calls handled by each agent on current date"
    # user_query = "give me count of calls received for billing yesterday"

    # user_query = "what is count of calls where call duration was more than 500 seconds and calls were abandoned"

    user_query = "Find the number of abandoned calls based on department for the latest available information but exclude prepay calls. I also need to know average, min and max time before the call is abandoned"

    #user_query = "what is count of calls where call duration was more than 500 seconds and calls were answered and calls were received for technical support yesterday"
    

    formatted_query = format_user_query(user_query)

    final_response = agent.process_query(formatted_query)

    print(f"Final Response: {final_response}")


if __name__ == "__main__":
    main()

