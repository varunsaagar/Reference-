
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
