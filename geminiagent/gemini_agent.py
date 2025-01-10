

import vertexai
from vertexai.generative_models import (
    FunctionDeclaration,
    GenerativeModel,
    Part,
    Tool,
    FunctionCall,  # Import FunctionCall
    Content,  # Import Content
)
from data_access import BigQueryManager
from typing import List, Dict, Tuple
import re
import json


class GeminiAgent:
    def __init__(self, project_id, dataset_id, table_id, location="us-central1"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        vertexai.init(project=project_id, location=location)

        self.bq_manager = BigQueryManager(project_id, dataset_id, table_id)

        # Retrieve and format the table schema
        self.table_schema = self.bq_manager.get_table_schema()
        self.formatted_table_schema = self._format_table_schema_for_prompt()

        # Initialize Gemini model
        self.model = GenerativeModel(
            "gemini-1.5-pro-002",
            tools=[self.bq_tool],
            generation_config={"temperature": 0},
        )

        self.chat = self.model.start_chat()

    def _extract_intents_and_entities(self, user_query: str) -> Tuple[str, Dict[str, List[str]]]:
        """
        Extracts intent and entities from the user query using Gemini model's understanding.
        This is an agentic approach, relying on the model's capabilities instead of predefined patterns.
        """

        prompt = f"""
        You are an expert at understanding user queries related to call center data.
        Analyze the following user query and extract the user's intent and relevant entities.

        User Query: '{user_query}'

        Possible Intents:
        - get_call_metrics
        - get_agent_performance
        - get_customer_info
        - get_call_details
        - get_transfer_info
        - get_abandon_info
        - general_query

        Possible Entities (extract values from the user query):
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

        Provide the output in valid JSON format with two keys: "intent" and "entities".
        "intent" should contain a single string representing the identified intent.
        "entities" should be a dictionary where keys are entity types (e.g., "DATE_RANGE")
        and values are lists of strings containing the extracted entity values.

        Do not provide any extra information apart from JSON response. Do not add `json at the starting of json response and ` at the end of the json response. 
        If no specific intent or entity is found, use "general_query" for intent and an empty dictionary for entities.

        Example:
        User Query: 'What was the average call duration for technical support calls yesterday?'
        Output:
        {{
          "intent": "get_call_metrics",
          "entities": {{
            "DATE_RANGE": ["yesterday"],
            "METRIC": ["call duration"],
            "TOPIC": ["technical support"]
          }}
        }}
        """

        response = self.model.generate_content(prompt)
        try:
            # Remove any leading/trailing whitespace and ```json from the response
            response_text = response.text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            
            # Parse the cleaned response as JSON
            response_json = json.loads(response_text)
            intent = response_json.get("intent", "general_query")
            entities = response_json.get("entities", {})
            return intent, entities

        except (json.JSONDecodeError, AttributeError):
            print(f"Error decoding intent/entity extraction response: {response.text}")
            return "general_query", {}          
    
    def _map_entities_to_columns_agentic(self, extracted_entities: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        Agentically maps extracted entities to their corresponding database columns using the Gemini model.
        """
        if not extracted_entities:
            return {}

        entity_mapping_prompt = f"""
        You are an expert in understanding the schema of a call center data table.
        You are given a set of entities extracted from a user query and the schema of the table.
        Your task is to map these entities to the most relevant columns in the table based on their meaning and context.

        Table Schema:\n{self.formatted_table_schema}

        Extracted Entities:\n{json.dumps(extracted_entities, indent=2)}

        Provide the output in JSON format where keys are entity types (e.g., "DATE_RANGE") and values are lists of corresponding column names from the table schema.
        
        Focus on semantic relevance:
          - "call duration" should map to "call_duration_seconds"
          - "handle time" should map to "handle_tm_seconds"
          - "hold time" should map to "hold_tm_seconds"
          - "talk time" should map to "talk_tm_seconds"
          - "ring time" should map to "ring_tm_seconds"
          - "delay time" should map to "delay_tm_seconds"
          - "abandon rate" should map to "abandons_cnt"
          - "call count" should map to "answered_cnt"
          - "DATE_RANGE" : ["call_end_dt", "call_answer_dt"]
          - "TOPIC" : ["acd_area_nm", "script_nm", "eccr_dept_nm", "bus_rule", "super_bus_rule"]
          - "CUSTOMER_SEGMENT" : ["icm_acct_type_cd", "cust_value"]
          - "AGENT_NAME/ID": [] 
          - "TRANSFER_STATUS" : ["transfer_flag", "transfer_point"]
          - "CALL_DISPOSITION" : ["final_call_dispo", "call_dispo_flag", "abandons_cnt", "answered_cnt"]
          - "BUSINESS_UNIT": ["eccr_line_bus_nm", "eccr_super_line_bus_nm"]
          - "CALL_CENTER": ["eccr_call_ctr_cd"]
          - "PHONE_NUMBER": ["mtn"]
          - "REGION": ["callers_region"]
          - "BUSINESS_RULE": ["bus_rule"]
          - "SUPER_BUSINESS_RULE": ["super_bus_rule"]
          - "SUPER_SKILL_GROUP": ["super_skill_group"]
          - "SUPER_CALL_TYPE": ["super_call_type"]

        If an entity type does not have a clear mapping, omit it from the output.
        If an entity type has multiple possible mappings, include all relevant columns.

        Example:
        Extracted Entities:
        {{
          "DATE_RANGE": ["yesterday"],
          "METRIC": ["call duration", "handle time"],
          "TOPIC": ["billing"]
        }}
        Output:
        {{
          "DATE_RANGE": ["call_end_dt"],
          "METRIC": ["call_duration_seconds", "handle_tm_seconds"],
          "TOPIC": ["eccr_dept_nm"]
        }}
        """

        response = self.model.generate_content(entity_mapping_prompt)
        try:
            response_json = json.loads(response.text)
            return response_json
        except (json.JSONDecodeError, AttributeError):
            print(f"Error decoding entity-column mapping response: {response.text}")
            return {}

    def _format_table_schema_for_prompt(self) -> str:
        """Formats the table schema into a string for the Gemini prompt."""
        if not self.table_schema:
            return ""

        formatted_schema = f"Table: {self.table_id}\n"
        for field in self.table_schema:
            formatted_schema += (
                f"- {field['name']}: {field['type']}"
                f"{' (' + field['description'] + ')' if field['description'] else ''}\n"
            )
        return formatted_schema

    def _generate_sql_prompt(
        self,
        user_query: str,
        intent: str,
        entity_mapping: Dict[str, List[str]],
        error_message: str = None,
    ) -> List[Part]:
        """Generates a prompt for the Gemini model to generate a SQL query, returning a list of Parts."""

        # Get the fully qualified table name
        full_table_name = f"`{self.project_id}.{self.dataset_id}.{self.table_id}`"

        # Basic prompt components, each formatted as a Part
        prompt_parts = [
            Part.from_text(
                "You are a helpful assistant that can convert natural language into SQL queries for Bigquery."
            ),
            Part.from_text(
                f"You have access to the following BigQuery table:\n{full_table_name}\n{self.formatted_table_schema}"
            ),
            Part.from_text(
                f"Convert the following natural language query into a SQL query:\n{user_query}"
            ),
            Part.from_text(f"Identified intent: {intent}"),
        ]

        # Add entity-column mapping information
        if entity_mapping:
            entity_mapping_text = "\nRelevant entities and their mappings to columns:"
            for entity_type, columns in entity_mapping.items():
                for column in columns:
                    entity_mapping_text += f"\n- {entity_type} -> {column}"
            prompt_parts.append(Part.from_text(entity_mapping_text))

        # Add error message if available
        if error_message:
            prompt_parts.append(
                Part.from_text(
                    f"\nPrevious SQL query generated an error: {error_message}\nPlease fix the SQL query based on this error message."
                )
            )

        # Add instructions for SQL generation
        instructions = """
        \nInstructions:
        - Generate a syntactically correct BigQuery SQL query.
        - Only use the table and columns mentioned in the schema.
        - Do not use table aliases unless necessary.
        - Use aggregate functions (COUNT, AVG, MIN, MAX, SUM) when appropriate.
        - Format dates and times correctly for comparisons.
        - Handle NULL values appropriately.
        - If a question is ambiguous, generate the most likely interpretation.
        - Make sure to add single quotes around the string values.
        - Dont add any extra information in the response apart from SQL query.
        - If you need to know the possible values in a column, use the `get_distinct_column_values` function to get a sample of distinct values.
        """
        prompt_parts.append(Part.from_text(instructions))

        # Add few-shot examples based on intent (can be expanded)
        if intent == "get_call_metrics":
            examples = """
            Examples:
            Question: How many calls were abandoned yesterday?
            SQL: SELECT COUNT(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact_exp` WHERE DATE(call_end_dt) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND abandons_cnt > 0

            Question: What is the average handle time for calls from the 'billing' department last week?
            SQL: SELECT AVG(handle_tm_seconds) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact_exp` WHERE eccr_dept_nm = 'billing' AND call_end_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            """
            prompt_parts.append(Part.from_text(examples))

        return prompt_parts

    def _generate_column_selection_examples(self) -> str:
        """
        Generates few-shot examples for column selection using the provided JSON data.
        """
        examples = ""
        for example in self.column_selection_examples:
            examples += f"""
            Context: {example['context']}
            Question: {example['question']}
            Thoughts: {example['thoughts']}
            Answer: {example['answer']}
            ---
            """
        return examples

    def _generate_column_selection_prompt(self, user_query: str) -> str:
        """
        Generates a prompt for the Gemini model to select relevant columns, including few-shot examples.
        """
        examples = self._generate_column_selection_examples()

        prompt = f"""
        You are an expert in selecting the most relevant columns from a SQL table based on a natural language question.
        You are given the table schema and a user question.
        Your task is to identify the columns that are most likely to be needed to answer the question.

        {examples}

        Table Schema:\n{self.formatted_table_schema}

        User Question: {user_query}

        Think step by step and select only the column names that are most relevant to answering the question.
        Provide the output as a comma-separated list of column names.
        """
        return prompt
        
    def _select_relevant_columns(self, user_query: str) -> List[str]:
        """
        Uses the Gemini model to select relevant columns for answering the user query.
        """
        prompt = self._generate_column_selection_prompt(user_query)
        response = self.model.generate_content(prompt)
        try:
            # Assuming the model returns a comma-separated list of column names
            columns = [
                col.strip() for col in response.text.split(",") if col.strip()
            ]
            return columns
        except (AttributeError, TypeError):
            print(f"Error processing column selection response: {response.text}")
            return []


    def _handle_function_call(self, response: Part) -> Part:
        """Handles function calls from the model."""
        function_name = response.function_call.name
        print(f"Model called function: {function_name}")

        if function_name == "get_table_schema":
            # For get_table_schema, prepare the response with the formatted schema
            function_response = Part.from_function_response(
                name=function_name,
                response={
                    "content": self.formatted_table_schema,
                },
            )
        elif function_name == "execute_sql_query":
            # For execute_sql_query, execute the query and return the results
            arguments = dict(response.function_call.args)
            sql_query = arguments["sql_query"]
            print(f"Executing SQL query: {sql_query}")
            try:
                query_results = self.bq_manager.execute_query(sql_query)
                function_response = Part.from_function_response(
                    name=function_name,
                    response={
                        "content": str(query_results),
                    },
                )
            except Exception as e:
                error_message = f"Error executing query: {e}"
                function_response = Part.from_function_response(
                    name=function_name, response={"content": error_message}
                )
        elif function_name == "get_distinct_column_values":
            # Handle the new function call to get distinct values
            arguments = dict(response.function_call.args)
            column_name = arguments["column_name"]
            limit = arguments.get("limit", 10)  # Default limit is 10
            print(f"Getting distinct values for column: {column_name} (limit: {limit})")

            try:
                distinct_values = self.bq_manager.get_distinct_values(
                    column_name, limit
                )
                function_response = Part.from_function_response(
                    name=function_name,
                    response={
                        "content": str(distinct_values),
                    },
                )
            except Exception as e:
                error_message = f"Error getting distinct values: {e}"
                function_response = Part.from_function_response(
                    name=function_name, response={"content": error_message}
                )
        else:
            raise ValueError(f"Unknown function: {function_name}")

        return function_response

    def _extract_sql_query(self, response_text: str) -> str:
        """
        Extracts the SQL query from the response text, removing only the
        beginning and ending triple backticks if they are part of a code block.
        """
        # Updated regex to correctly capture SQL query within triple backticks
        match = re.search(r"`sql\s*(.*?)\s*`", response_text, re.DOTALL)
        if match:
            sql_query = match.group(1).strip()
            print(f"Extracted SQL Query (before processing): {sql_query}")
            return sql_query
        else:
            print(f"No SQL query found in response: {response_text}")
            return ""

    def process_query(self, user_query: str, max_iterations: int = 5) -> str:
        """Processes the user query with iterative error correction."""
        # intent = self._extract_intent(user_query)
        # extracted_entities = self._extract_entities(user_query)
        # entity_mapping = self._map_entities_to_columns(extracted_entities)

        # Agentic intent and entity extraction
        intent, extracted_entities = self._extract_intents_and_entities(user_query)

        # Agentic entity-column mapping
        entity_mapping = self._map_entities_to_columns_agentic(extracted_entities)

        # Agentic column selection using the dedicated function
        selected_columns = self._select_relevant_columns(user_query)
        print(f"Selected columns: {selected_columns}")

        error_message = None

        for iteration in range(max_iterations):
            print(f"Iteration: {iteration + 1}")
            # Generate prompt based on user query, intent, entities, and any previous error
            sql_prompt_parts = self._generate_sql_prompt(
                user_query, intent, entity_mapping, error_message
            )

            # Send prompt to Gemini and handle function calls
            response = self.chat.send_message(sql_prompt_parts)
            print(f"Initial response: {response.candidates[0]}")

            while response.candidates[0].finish_reason == "TOOL":
                print(f"Function called in loop : {response.candidates[0].finish_reason}")
                function_response = self._handle_function_call(
                    response.candidates[0].content.parts[0]
                )
                response = self.chat.send_message(function_response)

            # Check if the model generated a SQL query
            if response.candidates[0].content.parts[0].text:
                # Extract SQL query, removing backticks
                sql_query = self._extract_sql_query(
                    response.candidates[0].content.parts[0].text
                )
                print(
                    f"Extracted SQL Query (before processing): {sql_query}"
                )  # Keep only one print statement

                # Directly execute the SQL query
                try:
                    query_results = self.bq_manager.execute_query(sql_query)
                    print(f"Query results: {query_results}")

                    # If query execution is successful, construct the response
                    if query_results:
                        return str(query_results)  # Return the results directly
                    else:
                        error_message = (
                            "Query executed successfully but returned no results."
                        )

                except Exception as e:
                    error_message = f"Error executing query: {e}"
                    print(error_message)

                # Update the chat history with the error for the next iteration (if there is one)
                if iteration < max_iterations - 1:
                    self.chat.history.append(
                        Content(
                            parts=[Part.from_text(f"Error: {error_message}")],
                            role="user",
                        )
                    )
                    self.chat.history.append(
                        Content(
                            parts=[
                                Part.from_text(
                                    f"Please provide the corrected SQL query for: {user_query}"
                                )
                            ],
                            role="model",
                        )
                    )
                else:
                    return (
                        error_message
                        if error_message
                        else "Max iterations reached without a successful query."
                    )

            else:
                # Handle cases where no SQL query is generated
                error_message = "No SQL query generated."
                print(error_message)
                return error_message

        return "Max iterations reached without a successful query."

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
