python3 main.py
Error decoding intent/entity extraction response: ```json
{
  "intent": "get_call_metrics",
  "entities": {
    "DATE_RANGE": ["yesterday"],
    "METRIC": ["count of calls"],
    "CALL_DISPOSITION": ["answered"],
    "TOPIC": ["technical support"],
    "TIME": ["500 seconds"]
  }
}
```
Selected columns: ['1. **call_duration_seconds**: To check if the call duration was more than 500 seconds.\n2. **answered_cnt**: To filter for calls that were answered.\n3. **eccr_dept_nm**: To filter calls related to technical support.\n4. **call_end_dt**: To filter calls received yesterday.\n\nAnswer: icm_summary_fact_exp.call_duration_seconds', 'icm_summary_fact_exp.answered_cnt', 'icm_summary_fact_exp.eccr_dept_nm', 'icm_summary_fact_exp.call_end_dt']


# utils.py

def format_sql_results(results):
    """Formats SQL query results into a human-readable string."""
    if not results:
        return "No results found."

    # Basic formatting:
    formatted_output = ""
    for row in results:
        formatted_output += str(row) + "\n"
    return formatted_output

def format_user_query(user_query):
    """
    Formats the user query for better model understanding.
    
    """
    # Remove extra whitespace and convert to lowercase
    user_query = user_query.strip().lower()

    return user_query
