import requests
import json
import os
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning from urllib3
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

url = "https://vegas-llm-test.ebiz.verizon.com/vegas/apps/prompt/v2/relay/use_case/text2sql/context/zero_shot_context"

payload = json.dumps({
    "contents": [
        {
            "parts": [
                {
                    "text": "hi"
                }
            ],
            "role": "user"
        }
    ],
    "system_instruction": {
        "parts": [
            {
                "text": "You are the CFO. Using the critique agent's feedback, enhance the initial QBR report and generate the final version."
            }
        ]
    }
})
headers = {
    'Content-Type': 'application/json'
}

# Get proxy settings from environment variables
http_proxy = os.environ.get('http_proxy')
https_proxy = os.environ.get('https_proxy')

# Configure proxy settings for requests
proxies = {
    "http": http_proxy,
    "https": https_proxy,
}



# Make the request using the configured proxies
try:
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies, verify=False)
except requests.exceptions.ProxyError as e:
    print(f"Error: Could not connect to proxy: {e}")
    print("Please verify that the proxy settings are correct and the proxy server is reachable.")
    exit()
except requests.exceptions.SSLError as e:
    print(f"Error: SSL verification failed: {e}")
    print("If using a self-signed certificate, ensure it's added to the trusted store or consider using 'verify=False' (not recommended for production).")
    exit()
except requests.exceptions.ConnectionError as e:
    print(f"Error: Could not connect to the URL: {e}")
    print("Check network connectivity and the URL's availability.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit()

# Print the response
print(response.text)
import requests
import json
import re
import numpy as np
import faiss

from typing import List, Dict, Tuple
from data_access import BigQueryManager
from create_vector_db import get_embeddings


class VegasAgent:
    """
    This class replaces the previous Gemini-based agent usage with
    a Vegas LLM API wrapper. All references to Gemini/VertexAI
    have been removed or replaced. This version uses a REST call
    to the Vegas endpoint.
    """

    def __init__(self, project_id, dataset_id, location="us-central1"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        # self.table_id will remain dynamic, determined at runtime
        self.table_id = None
        self.location = location

        # Initialize BigQuery manager
        self.bq_manager = BigQueryManager(project_id, dataset_id)

        # FAISS-based semantic search setup
        self.index = faiss.read_index("call_center_embeddings.faiss")
        with open("index_data.json", "r") as f:
            self.index_data = json.load(f)

        # Vegas API endpoint (example)
        self.vegas_url = (
            "https://vegas-llm-test.ebiz.verizon.com/vegas/apps/prompt/v2/"
            "relay/use_case/text2sql/context/zero_shot_context"
        )

        # Placeholders for table schema
        self.table_schema = []
        self.formatted_table_schema = ""

        # Example: You might store a default system instruction here, or modify at runtime.
        self.default_system_text = (
            "You are a helpful assistant that can analyze user queries related to BigQuery data."
        )

        # Example column selection usage
        self.column_selection_examples = [
            {
                "context": """
                Table Name: icm_summary_fact_exp (example schema omitted for brevity)
                """,
                "question": "What was the average call duration for technical support calls yesterday?",
                "thoughts": [
                    "Relevant columns: call_duration_seconds, eccr_dept_nm, call_end_dt."
                ],
                "answer": "icm_summary_fact_exp.call_duration_seconds, icm_summary_fact_exp.eccr_dept_nm, icm_summary_fact_exp.call_end_dt",
            },
            {
                "context": """
                Same Table: icm_summary_fact_exp
                """,
                "question": "How many calls were abandoned yesterday?",
                "thoughts": [
                    "Relevant columns: abandons_cnt, call_end_dt."
                ],
                "answer": "icm_summary_fact_exp.abandons_cnt, icm_summary_fact_exp.call_end_dt",
            },
            # ... Additional examples omitted for brevity ...
        ]

    def _call_vegas_api(self, user_message: str, system_message: str = None) -> str:
        """
        Makes a single-turn call to the Vegas LLM API.
        user_message: The user content to send to the LLM.
        system_message: An optional system instruction or role content.
        Returns the model-generated text.
        """
        if not system_message:
            system_message = self.default_system_text

        # Construct the payload in line with the Vegas API format
        payload = {
            "contents": [
                {
                    "parts": [{"text": user_message}],
                    "role": "user"
                }
            ],
            "system_instruction": {
                "parts": [{"text": system_message}]
            }
        }
        headers = {"Content-Type": "application/json"}

        # Make the request
        try:
            response = requests.post(
                self.vegas_url, headers=headers, data=json.dumps(payload), timeout=60
            )
            response_json = response.json()
        except Exception as e:
            print(f"Error calling Vegas API: {e}")
            return ""

        # Parse out the response text from the first candidate
        try:
            return response_json["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            return ""

    def _select_table(self, user_query: str) -> str:
        """
        Uses the Vegas LLM to select the most relevant table based on user query.
        Previously used Gemini; now replaced with a single-turn Vegas LLM call.
        """
        table_descriptions = self.bq_manager.get_table_descriptions()
        prompt = (
            "You are a table selection agent. You are given a user query and a set of "
            "tables with their descriptions. Your task is to determine which table is "
            "most likely to contain the information needed to answer the query.\n\n"
            f"User Query: '{user_query}'\n\n"
            "Available Tables:\n"
        )
        for table_name, description in table_descriptions.items():
            prompt += f"- **{table_name}**: {description}\n"

        prompt += (
            "\nBased on the user query and the table descriptions, please provide the name "
            "of the most relevant table. Return only the table name and nothing else."
        )

        response_text = self._call_vegas_api(prompt)
        selected_table = response_text.strip()
        print(f"Selected Table: {selected_table}")
        return selected_table

    def _extract_intents_and_entities(self, user_query: str) -> Tuple[str, Dict[str, List[str]]]:
        """
        Extracts intent and entities from the user query using the Vegas LLM.
        This replaces the previous Gemini-based approach.
        """
        prompt = f"""
You are an expert at understanding user queries related to call center data.
Analyze the following user query and extract the user's intent and relevant entities.

User Query: '{user_query}'

## Intent

The user's intent should be a single string representing the primary goal of the query.
Choose one of the following possible intents:

Possible Intents:
- get_call_metrics
- get_agent_performance
- get_customer_info
- get_call_details
- get_transfer_info
- get_abandon_info
- general_query

If no specific intent is found, use "general_query".

## Entities

Entities are specific pieces of information within the user query that are relevant to the intent.
Extract entity values directly from the user query. **Do not include column names.**

Possible Entity Types:
- DATE_RANGE
- TIME
- METRIC
- TOPIC
- AGENT_NAME
- CUSTOMER_SEGMENT
- TRANSFER_STATUS
- CALL_DISPOSITION
- BUSINESS_UNIT
- CALL_CENTER
- PHONE_NUMBER
- REGION
- BUSINESS_RULE
- SUPER_BUSINESS_RULE
- SUPER_SKILL_GROUP
- SUPER_CALL_TYPE

## Output Format

Provide the output in **valid JSON format** with **exactly two keys**: "intent" and "entities".

- **"intent"**: A string representing the identified intent.
- **"entities"**: A dictionary where:
    - Keys are entity types (e.g., "DATE_RANGE", "METRIC").
    - Values are lists of strings containing the extracted entity values.

**Strictly follow the JSON format. Do not deviate.
Do not include any additional text or explanation outside of the JSON object.
Do not include backticks (```) or the word "json" in your response. Do not put any newlines or spaces at the beginning or end of your response. Do not use escape characters like '\\n' in your response. Do not hallucinate any entity value which is not present in user query. Under no circumstances should you break any of the above instructions.**

Examples
Example 1: User Query: 'What was the average call duration for technical support calls yesterday?' Output: {{"intent": "get_call_metrics", "entities": {{"DATE_RANGE": ["yesterday"], "METRIC": ["call duration"], "TOPIC": ["technical support"]}}}}

Example 2: User Query: 'How many calls were abandoned last week?' Output: {{"intent": "get_call_metrics", "entities": {{"DATE_RANGE": ["last week"], "CALL_DISPOSITION": ["abandoned"]}}}}

Now, analyze the user query provided at the beginning and provide the intent and entities in the specified JSON format. """
        response_text = self._call_vegas_api(prompt)
        try:
            response_json = json.loads(response_text)
            intent = response_json.get("intent", "general_query")
            entities = response_json.get("entities", {})
            return intent, entities
        except (json.JSONDecodeError, AttributeError):
            print(f"Error decoding intent/entity extraction response: {response_text}")
            return "general_query", {}

    def _map_entities_to_columns_agentic(self, extracted_entities: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        Agentic approach using the Vegas LLM to map extracted entities to corresponding columns.
        """
        if not extracted_entities:
            return {}
        # We'll assume self.formatted_table_schema has some textual description
        entity_mapping_prompt = f"""
You are an expert in understanding the schema of a call center data table. You are given a set of entities extracted from a user query and the schema of the table. Your task is to map these entities to the most relevant columns in the table based on their meaning and context.

Table Schema: {self.formatted_table_schema}

Extracted Entities: {json.dumps(extracted_entities, indent=2)}

Provide the output in valid JSON format where keys are entity types and values are lists of corresponding column names. Do not include any additional text or explanation outside of the JSON object. Do not include backticks (```)

Do not put any newlines or spaces at the beginning or end of your response.
Do not use escape characters like '\\n' in your response.
Under no circumstances should you break any of the above instructions.

Map known METRIC keywords to columns, for example:
- "call duration" -> "call_duration_seconds"
- "handle time" -> "handle_tm_seconds"
- "abandon" -> "abandons_cnt"
- etc.
        """

        response_text = self._call_vegas_api(entity_mapping_prompt)
        try:
            response_json = json.loads(response_text)
            return response_json
        except (json.JSONDecodeError, AttributeError):
            print(f"Error decoding entity-column mapping response: {response_text}")
            return {}

    def _semantic_search_columns(self, user_query: str) -> List[str]:
        """
        Performs semantic search using FAISS to find relevant columns and values.
        Returns the most relevant columns based on similarity scores.
        """
        try:
            # Embed the query
            query_embedding = get_embeddings([user_query])[0]
            query_embedding = np.array([query_embedding], dtype=np.float32).reshape(1, -1)
            faiss.normalize_L2(query_embedding)

            # Search top 10
            D, I = self.index.search(query_embedding, k=10)

            column_scores = {}

            for score, idx in zip(D[0], I[0]):
                if idx >= len(self.index_data):
                    continue

                data = self.index_data[idx]
                similarity = (1 + score) / 2  # Convert [-1,1] to [0,1]
                if data["type"] in ["column", "value", "column_description"]:
                    col_name = f"{data['table']}.{data['column']}"
                    if col_name not in column_scores or similarity > column_scores[col_name]:
                        column_scores[col_name] = similarity

            threshold = 0.3
            relevant_columns = [
                col for col, sc in column_scores.items() if sc > threshold
            ]
            relevant_columns.sort(key=lambda x: column_scores[x], reverse=True)
            return relevant_columns
        except Exception as e:
            print(f"Error in semantic search: {e}")
            return []

    def _format_table_schema_for_prompt(self) -> str:
        """
        Formats the table schema so it can be included in LLM prompts.
        """
        if not self.table_schema:
            return ""

        formatted_schema = f"Table: {self.table_id}\n"
        for field in self.table_schema:
            col_desc = (
                f" ({field['description']})" if field.get("description") else ""
            )
            formatted_schema += f"- {field['name']}: {field['type']}{col_desc}\n"
        return formatted_schema

    def _generate_sql_prompt(
        self,
        user_query: str,
        intent: str,
        entity_mapping: Dict[str, List[str]],
        error_message: str = None,
    ) -> str:
        """
        Generates a single text prompt for the Vegas LLM to produce a SQL query.
        Previously used multiple parts with Gemini; now collapsed into a single string.
        """

        full_table_name = f"`{self.project_id}.{self.dataset_id}.{self.table_id}`"

        # Start building the prompt
        prompt = (
            "You are a helpful assistant that can convert natural language into SQL queries for BigQuery.\n\n"
            f"You have access to the following BigQuery table:\n{full_table_name}\n\n"
            f"Table Schema:\n{self.formatted_table_schema}\n\n"
            f"Convert the following natural language query into a SQL query:\n{user_query}\n\n"
            f"Identified intent: {intent}\n"
        )

        # Add entity-to-column mapping
        if entity_mapping:
            prompt += "\nRelevant entities mapped to columns:\n"
            for entity_type, columns in entity_mapping.items():
                prompt += f" - {entity_type}: {columns}\n"

        # If there's an error from a previous attempt
        if error_message:
            prompt += (
                f"\nThe previous SQL query caused an error: {error_message}\n"
                "Please generate a corrected SQL query.\n"
            )

        # Basic instructions for BigQuery SQL
        prompt += """
Instructions:
- Generate a syntactically correct BigQuery SQL query.
- Only use the table and columns mentioned in the schema.
- Do not use table aliases unless necessary.
- Use aggregate functions (COUNT, AVG, MIN, MAX, SUM) when appropriate.
- Format dates and times correctly for comparisons.
- Handle NULL values appropriately.
- If a question is ambiguous, generate the most likely interpretation.
- Make sure to add single quotes around string values.
- Do not add any explanations, only provide the final SQL query.
If you need possible values in a column, mention it or ask for them explicitly.
"""

        return prompt

    def _select_relevant_columns(self, user_query: str) -> List[str]:
        """
        Uses the Vegas LLM to select relevant columns for the user query.
        This is a simplified approach, previously done via multi-part Gemini chat.
        Here, we build a single prompt with examples.
        """
        # Build few-shot examples from self.column_selection_examples
        examples = ""
        for example in self.column_selection_examples:
            examples += f"""
Context: {example['context']}
Question: {example['question']}
Thoughts: {example['thoughts']}
Answer: {example['answer']}
---
"""

        prompt = (
            "You are an expert in selecting the most relevant columns from a SQL table "
            "based on a natural language question. You are given the table schema and "
            "a user question. Your task is to identify the columns that are most likely "
            "to be needed to answer the question.\n\n"
            f"{examples}\n"
            f"Table Schema:\n{self.formatted_table_schema}\n\n"
            f"User Question: {user_query}\n\n"
            "Think step by step and select only the column names that are most relevant.\n"
            "Provide the output as a comma-separated list of column names (no quotes, no brackets):"
        )

        response_text = self._call_vegas_api(prompt)
        # Attempt to split on commas:
        columns = [col.strip() for col in response_text.split(",") if col.strip()]
        return columns

    def _extract_sql_query(self, response_text: str) -> str:
        """
        Attempt to extract a SQL query from triple backticks if present;
        otherwise, return the text as-is.
        """
        match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return response_text.strip()

    def process_query(self, user_query: str, max_iterations: int = 3) -> str:
        """
        Main entry point to process a user query:
         1) Select the relevant table
         2) Extract intent and entities
         3) Map entities to columns
         4) Perform semantic search for columns
         5) Generate a SQL query
         6) Execute it with BigQuery
         7) Summarize the results
        """

        # 1) Select table
        self.table_id = self._select_table(user_query)
        if not self.table_id:
            return "Could not determine the appropriate table for the query."
        print(f"Selected table id: {self.table_id}")

        # 2) Retrieve table schema for prompts
        self.bq_manager.table_id = self.table_id
        self.table_schema = self.bq_manager.get_table_schema(self.table_id)
        self.formatted_table_schema = self._format_table_schema_for_prompt()

        # 3) Intent & entity extraction
        intent, extracted_entities = self._extract_intents_and_entities(user_query)

        # 4) Entity to column mapping
        entity_mapping = self._map_entities_to_columns_agentic(extracted_entities)

        # 5) Column selection
        selected_columns = self._select_relevant_columns(user_query)
        print(f"Selected columns: {selected_columns}")

        # 6) Semantic search to refine column selection
        relevant_semantic_cols = self._semantic_search_columns(user_query)
        selected_columns = list(set(selected_columns + relevant_semantic_cols))
        print(f"Combined selected columns after semantic search: {selected_columns}")

        # Attempt iterative SQL generation & correction
        error_message = None
        for iteration in range(max_iterations):
            print(f"Iteration {iteration + 1} of {max_iterations}...")
            sql_prompt = self._generate_sql_prompt(user_query, intent, entity_mapping, error_message)
            response_text = self._call_vegas_api(sql_prompt)

            if not response_text:
                error_message = "No SQL query generated or empty response from Vegas."
                print(error_message)
                if iteration == max_iterations - 1:
                    return error_message
                continue

            # 7) Extract SQL and try executing
            sql_query = self._extract_sql_query(response_text)
            print(f"Attempted SQL query:\n{sql_query}")

            try:
                query_results = self.bq_manager.execute_query(sql_query)
                if query_results:
                    # 8) Summarize results for business
                    summary = self._generate_business_summary(user_query, intent, extracted_entities, query_results)
                    return summary
                else:
                    error_message = "Query executed but returned no results."
                    print(error_message)
            except Exception as e:
                error_message = f"Error executing query: {e}"
                print(error_message)

            if iteration == max_iterations - 1:
                return "Max iterations reached without a successful query."

        return "Max iterations reached without a successful query."

    def _generate_business_summary(self, user_query: str, intent: str, entities: Dict, results: List[Dict]) -> str:
        """
        Summarizes the query results for a business audience using the Vegas LLM.
        """
        if not results:
            return "No results found for your query."

        prompt = f"""
You are an expert in summarizing SQL query results for a business audience.

User Query: {user_query}
Query Results: {results}

Provide a concise, human-readable summary that explains what the user asked and the key findings
from the data. Keep it free of low-level technical details or unnecessary verbiage.
"""
        response_text = self._call_vegas_api(prompt)
        summary = response_text.strip()
        if not summary:
            summary = "A summary could not be generated based on the available information."
        return summary
     
    # Define the function declarations
    get_table_schema_func = FunctionDeclaration(
        name="get_table_schema",
        description="Get the schema of the specified table.",
        parameters={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "The fully qualified name of the table in the format `project_id.dataset_id.table_id`",
                },
            },
            "required": ["table_name"],
        },
    )

    execute_sql_query_func = FunctionDeclaration(
        name="execute_sql_query",
        description="Execute a SQL query against the BigQuery database and get the result as json list.",
        parameters={
            "type": "object",
            "properties": {
                "sql_query": {
                    "type": "string",
                    "description": "The SQL query to execute.",
                }
            },
            "required": ["sql_query"],
        },
    )

    # Add new function declaration for getting distinct values
    get_distinct_values_func = FunctionDeclaration(
        name="get_distinct_column_values",
        description="Get a sample of distinct values from a specified column in the table.",
        parameters={
            "type": "object",
            "properties": {
                "column_name": {
                    "type": "string",
                    "description": "The name of the column to get distinct values from.",
                },
                "limit": {
                    "type": "integer",
                    "description": "The maximum number of distinct values to return.",
                    "default": 10,
                },
            },
            "required": ["column_name"],
        },
    )

    # Create the tool that includes the function declarations
    bq_tool = Tool(
        function_declarations=[
            get_table_schema_func,
            execute_sql_query_func,
            get_distinct_values_func,
        ],
    )

    column_selection_examples = [
        {
            "context": """
            Table Name: icm_summary_fact_exp
            This table has the following columns:
            1. recoverykey: STRING (The unique identifier for the customer's recovery key.)
            2. ivr_call_id: STRING (A unique identifier for the call in the Interactive Voice Response (IVR) system.)
            3. acd_area_nm: STRING (Automatic Call Delivery area name of the caller, ie IVR Queue Name/Department Example - Prepay,SPC,WestCentral,Federal Accounts etc. Area code nulls indicate Internal)
            4. call_end_dt: DATE (The date and time when the call ended.)
            5. call_end_tm: TIME (The time the call ended.)
            6. call_answer_tm: STRING (Time it took for the call to be answered in seconds.)
            7. route_value: STRING (The value of the route that was used to direct the call.)
            8. icm_acct_type_cd: STRING (The ICM (Intelligent Contact Manager) account type code identifies the type of account that the caller has.)
            9. eqp_prod_id: STRING (The equipment product ID of the caller's device.)
            10. cust_value: STRING (Value of the customer to Verizon Wireless, based on factors such as their usage patterns, loyalty, and profitability.)
            11. lang_pref_ind: STRING (The language preference indicator identifies the preferred language of the caller.)
            12. cacs_state_cd: STRING (The state of the customer's account in the Computer Assisted Collection System (CACS).)
            13. first_bill_cd: STRING (The first bill code associated with the call.)
            14. onstar_ind: STRING (Indicator for whether the call was made through OnStar.)
            15. transfer_point: STRING (The point at which the call was transferred to another agent.)
            16. onebill_ind: STRING (An indicator that specifies whether the customer is enrolled in the One Bill program.)
            17. high_risk_ind: STRING (Indicates whether the call was high-risk.)
            18. cacs_work_state_cd: STRING (Code indicating the work state of the Computer Assisted Collection System (CACS) at the time of the call.)
            19. ivr_cust_src_cd: STRING (The source of the customer's call to the Interactive Voice Response (IVR) system.)
            20. bus_rule: STRING (The business rule that was applied to the customer's account.)
            21. script_nm: STRING (The name of the script that was used to handle the customer's call.)
            22. eccr_line_bus_nm: STRING (ECCR line business name.)
            23. eccr_super_line_bus_nm: STRING (The name of the business unit that owns the ECCR super line.)
            24. eccr_dept_nm: STRING (The name of the Enterprise Contact Center Reporting (ECCR) department. Call centers agents are mapped to department to handle appropriate calls based on their specialization)
            25. mtn: STRING (The mobile telephone number of the caller.)
            26. eccr_call_ctr_cd: STRING (The enterprise contact center reporting (ECCR) call center code identifies the call center that handled the call.)
            27. acd_appl_id: INT64 (The identifier of the Automatic Call Delivery (ACD) application that was used to route the call.)
            28. agent_group_id: INT64 (The identifier of the agent group that handled the call.)
            29. callers_region: STRING (The region of the caller.)
            30. transfer_flag: STRING (Indicates whether the call was transferred.)
            31. final_call_dispo: INT64 (The final disposition of the call, indicating the outcome of the call attempt. Possible values include 'ANSWERED', 'BUSY', 'FAILED', 'NO ANSWER', and 'UNKNOWN'.)
            32. call_dispo_flag: INT64 (A flag that indicates the disposition of the call, such as whether it was answered, abandoned, or transferred.)
            33. peripheral_call_type: INT64 (The type of peripheral call.)
            34. final_object_id: INT64 (The final object ID associated with the call.)
            35. call_duration_seconds: INT64 (The duration of the call, in seconds.)
            36. ring_tm_seconds: INT64 (The ring time in seconds is the amount of time that the phone rang before the caller answered.)
            37. delay_tm_seconds: INT64 (The delay time in seconds is the additional time that has been requested for response.)
            38. time_to_aband_seconds: INT64 (The time it took for the caller to abandon the call, in seconds.)
            39. hold_tm_seconds: INT64 (The hold time in seconds is the amount of time that the caller spent on hold during the call.)
            40. talk_tm_seconds: INT64 (Total talk time for the call in seconds.)
            41. work_tm_seconds: INT64 (The total amount of time spent on the call, in seconds, including talk time, hold time, and other activities.)
            42. local_q_tm_seconds: INT64 (The local queue time in seconds is the amount of time that the caller spent in the queue before their call was answered.)
            43. handle_tm_seconds: INT64 (The total time in seconds that the call was handled.)
            44. delay_answer_seconds: INT64 (The amount of time, in seconds, that the call was delayed before being answered by an agent.)
            45. call_offered_cnt: INT64 (The number of times the call was offered to an agent.)
            46. answer_half_hr: INT64 (The time it took to answer the call, in half-hour increments.)
            47. abandons_cnt: INT64 (The number of abandoned calls. Values 0, 1)
            48. answered_cnt: INT64 (Indicator to denote if the call was answered or not)
            49. ansr_30_cnt: INT64 (The number of calls that were answered within 30 seconds.)
            50. ansr_30_to_40_cnt: INT64 (The number of calls that were answered within 30 to 40 seconds.)
            51. tm_zone_offset: INT64 (The time zone offset of the caller.)
            52. callcenterid: INT64 (Identifier for the call center that handled the call.)
            53. sor_id: STRING (Identifier for the source system of record that is populating the data warehouse instance. Always use 'V')
            54. cust_id: STRING (The unique identifier for the customer.)
            55. cust_line_seq_id: STRING (The sequence ID of the customer line.)
            56. acss_call_id: STRING (The unique identifier for the call in the Automated Customer Support System (ACSS).)
            57. callcenterid_agent: INT64 (The call center ID of the agent who handled the call.)
            58. acd_area_nm_agent: STRING (The ACD (Automatic Call Delivery) area name agent identifies the area of the call center that handled the call.)
            59. eccr_line_bus_nm_agent: STRING (Enterprise Contact Center Reporting Line Business Name Agent (Eccr Line Bus Nm Agent) identifies the business unit or line of business associated with the agent who handled the call, providing context on the agent's area of expertise or specialization.)
            60. eccr_dept_nm_agent: STRING (The ECCR (Enterprise Contact Center Reporting) department name agent identifies the department of the call center that handled the call.)
            61. rep_type_cd: STRING (Indicates the type of representative who handled the call, such as a customer service representative, technical support representative, or sales representative.)
            62. ecc_sm_ind: STRING (An indicator that specifies whether the call was handled by the Ebonding Collaboration Center (ECC) Switching Module (SM).)
            63. eid: STRING (The Endpoint Identifier assigned to the customer.)
            64. call_answer_dt: DATE (The call answer date is the date on which the call was answered.)
            65. routercallkeyday: INT64 (router call key day)
            66. routercallkey: STRING (router call key)
            67. super_bus_rule: STRING (super busines rule name)
            68. super_skill_group: STRING (super skill group name)
            69. super_call_type_cd: STRING (name of super call type)
            70. dev_cat_cd: STRING (dev category code)
            71. hpr_cd: STRING (high priority code)
            72. specialization_cd: STRING (specialization code)
            73. client_channel_cd: STRING (client channel code)
            74. client_application_cd: STRING (client application code)
            75. call_status: STRING (call status)
            76. call_reas_cd: STRING (call reason code)
            77. orig_ivr_call_id: STRING (original ivr call identifier)
            """,
            "question": "What was the average call duration for technical support calls yesterday?",
            "thoughts": [
                "The question is asking for a metric (average call duration) related to a specific type of call (technical support) on a specific day (yesterday).",
                "The table 'icm_summary_fact_exp' seems to contain call-related data, and it has columns related to call duration, call type, and date.",
                "The 'call_duration_seconds' column likely contains the duration of each call.",
                "The 'eccr_dept_nm' column could contain information about the department, such as 'technical support'.",
                "The 'call_end_dt' column contains the date when the call ended, which can be used to filter for 'yesterday'.",
            ],
            "answer": "icm_summary_fact_exp.call_duration_seconds, icm_summary_fact_exp.eccr_dept_nm, icm_summary_fact_exp.call_end_dt",
        },
        {
            "context": """
            Table Name: icm_summary_fact_exp
            This table has the following columns:
            (Refer to previous definition for column details)
            """,
            "question": "How many calls were abandoned yesterday?",
            "thoughts": [
                "The question is asking for a count of a specific type of call (abandoned calls) on a specific day (yesterday).",
                "The table 'icm_summary_fact_exp' seems to contain call-related data, including information about whether a call was abandoned.",
                "The 'abandons_cnt' column likely indicates whether a call was abandoned (it is mentioned that it has values 0 or 1).",
                "The 'call_end_dt' column contains the date when the call ended, which can be used to filter for 'yesterday'.",
            ],
            "answer": "icm_summary_fact_exp.abandons_cnt, icm_summary_fact_exp.call_end_dt",
        },
        {
            "context": """
            Table Name: icm_summary_fact_exp
            This table has the following columns:
            (Refer to previous definition for column details)
            """,
            "question": "How many calls were received for billing last month?",
            "thoughts": [
                "The question is asking for a count of calls related to a specific department (billing) in a specific time period (last month).",
                "The table 'icm_summary_fact_exp' contains call-related data, including information about the department and the date.",
                "The 'eccr_dept_nm' column likely contains the department name, such as 'billing'.",
                "The 'call_end_dt' column contains the date when the call ended, which can be used to filter for 'last month'.",
                "The 'answered_cnt' column can be used to count calls.",
            ],
            "answer": "icm_summary_fact_exp.eccr_dept_nm, icm_summary_fact_exp.call_end_dt, icm_summary_fact_exp.answered_cnt",
        },
        {
            "context": """
            Table Name: icm_summary_fact_exp
            This table has the following columns:
            (Refer to previous definition for column details)
            """,
            "question": "What was the average call handling time for billing inquiries in the last week, broken down by agent?",
            "thoughts": [
                "The question is asking for a metric (average call handling time) for a specific type of call (billing inquiries) in a specific time period (last week), grouped by agent.",
                "The table 'icm_summary_fact_exp' contains call-related data, including information about handling time, department, agent, and date.",
                "The 'handle_tm_seconds' column likely contains the call handling time.",
                "The 'eccr_dept_nm' column could contain information about the department, such as 'billing'.",
                "The 'eid' column likely represents the agent ID.",
                "The 'call_end_dt' column contains the date when the call ended, which can be used to filter for 'last week'.",
            ],
            "answer": "icm_summary_fact_exp.handle_tm_seconds, icm_summary_fact_exp.eccr_dept_nm, icm_summary_fact_exp.eid, icm_summary_fact_exp.call_end_dt",
        },
        {
            "context": """
            Table Name: icm_summary_fact_exp
            This table has the following columns:
            (Refer to previous definition for column details)
            """,
            "question": "Find the number of abandoned calls based on department for the latest available information but exclude prepay calls.",
            "thoughts": [
                "The question is asking for a count of a specific type of call (abandoned calls) grouped by department, excluding a certain category (prepay calls) and considering only the latest available data.",
                "The table 'icm_summary_fact_exp' contains call-related data, including information about call disposition, department, and date.",
                "The 'abandons_cnt' column indicates whether a call was abandoned.",
                "The 'eccr_dept_nm' column likely contains the department name.",
                "The 'acd_area_nm' column could contain information to filter out 'prepay' calls.",
                "The 'call_end_dt' column contains the date when the call ended, which can be used to filter for the latest available information.",
            ],
            "answer": "icm_summary_fact_exp.abandons_cnt, icm_summary_fact_exp.eccr_dept_nm, icm_summary_fact_exp.acd_area_nm, icm_summary_fact_exp.call_end_dt",
        },
    ]
