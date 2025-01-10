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

        Provide the output in JSON format with two keys: "intent" and "entities".
        "intent" should contain a single string representing the identified intent.
        "entities" should be a dictionary where keys are entity types (e.g., "DATE_RANGE")
        and values are lists of strings containing the extracted entity values.

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
            response_json = json.loads(response.text)
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
            "context": "\n  Table Name: weather\n  This table has the following columns :\n     1. date\n        This column is of type TEXT and is nullable.\n     2. max_temperature_f\n        This column is of type INTEGER and is nullable.\n     3. mean_temperature_f\n        This column is of type INTEGER and is nullable.\n     4. min_temperature_f\n        This column is of type INTEGER and is nullable.\n     5. max_dew_point_f\n        This column is of type INTEGER and is nullable.\n     6. mean_dew_point_f\n        This column is of type INTEGER and is nullable.\n     7. min_dew_point_f\n        This column is of type INTEGER and is nullable.\n     8. max_humidity\n        This column is of type INTEGER and is nullable.\n     9. mean_humidity\n        This column is of type INTEGER and is nullable.\n     10. min_humidity\n        This column is of type INTEGER and is nullable.\n     11. max_sea_level_pressure_inches\n        This column is of type NUMERIC and is nullable.\n     12. mean_sea_level_pressure_inches\n        This column is of type NUMERIC and is nullable.\n     13. min_sea_level_pressure_inches\n        This column is of type NUMERIC and is nullable.\n     14. max_visibility_miles\n        This column is of type INTEGER and is nullable.\n     15. mean_visibility_miles\n        This column is of type INTEGER and is nullable.\n     16. min_visibility_miles\n        This column is of type INTEGER and is nullable.\n     17. max_wind_Speed_mph\n        This column is of type INTEGER and is nullable.\n     18. mean_wind_speed_mph\n        This column is of type INTEGER and is nullable.\n     19. max_gust_speed_mph\n        This column is of type INTEGER and is nullable.\n     20. precipitation_inches\n        This column is of type INTEGER and is nullable.\n     21. cloud_cover\n        This column is of type INTEGER and is nullable.\n     22. events\n        This column is of type TEXT and is nullable.\n     23. wind_dir_degrees\n        This column is of type INTEGER and is nullable.\n     24. zip_code\n        This column is of type INTEGER and is nullable.\n",
            "question": "What are the date, mean temperature, mean rainfall and mean humidity for the top 3 days with the largest max gust speeds and lowest average snowfall",
            "thoughts": [
                "The table seems to describe various weather attributes for each day for each zip code, and the question is asking about certain weather attributes under specific conditions.",
                "The question mentions \"date\", and the table contains date information in the \"date\" column.",
                "The question mentions \"mean temperature\" and the information about average temperatures is present in the column \"mean_temperature_f\".",
                "The question mentions \"mean rainfall\", but the table does not contain any information about rainfall.",
                "The question mentions \"mean humidity\" and the information about average humidity is present in the column \"mean_humidity\".",
                "The question mentions \"top 3 days\", and the information about days and dates is present in the column \"date\".",
                "The question mentions \"largest max gust speeds\", and the inormation about gust speeds is present in the column \"max_gust_speed_mph\".",
                "The question mentions \"average snowfall\", but the table does not contain information about snowfall.",
            ],
            "answer": "weather.date, weather.mean_humidity, weather.mean_temperature_f, weather.max_gust_speed_mph",
        },
        {
            "context": "\n  Table Name: races\n  This table has the following columns :\n     1. raceId\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. year\n        This column is of type INTEGER and is nullable.\n     3. round\n        This column is of type INTEGER and is nullable.\n     4. circuitId\n        This column is of type INTEGER and is nullable.\n     5. name\n        This column is of type TEXT and is nullable.\n     6. date\n        This column is of type TEXT and is nullable.\n     7. time\n        This column is of type TEXT and is nullable.\n     8. url\n        This column is of type TEXT and is nullable.\n",
            "question": "Give me a list of names and years of races that had any driver whose forename is Lewis?",
            "thoughts": [
                "The table seems to describe various details for each race, and the question is asking about details of races which had drivers with a certain forename.",
                "The question mentions \"names\" and the information about name is present in the column \"name\".",
                "The question mentions \"years\" and the information about year is present in the column \"year\".",
                "The question mentions \"forename\" and the information about forename is present in the column \"name\".",
            ],
            "answer": "races.year, races.name, races.raceid",
        },
        {
            "context": "\n  Table Name: wine\n  This table has the following columns :\n     1. No\n        This column is of type INTEGER and is nullable.\n     2. Grape\n        This column is of type TEXT and is nullable.\n     3. Winery\n        This column is of type TEXT and is nullable.\n     4. Appelation\n        This column is of type TEXT and is nullable.\n     5. State\n        This column is of type TEXT and is nullable.\n     6. Name\n        This column is of type TEXT and is nullable.\n     7. Year\n        This column is of type INTEGER and is nullable.\n     8. Price\n        This column is of type INTEGER and is nullable.\n     9. Score\n        This column is of type INTEGER and is nullable.\n     10. Cases\n        This column is of type INTEGER and is nullable.\n     11. Drink\n        This column is of type TEXT and is nullable.\n",
            "question": "What are the names and scores of wines that are made of white color grapes?",
            "thoughts": [
                "The table seems to describe various wine attributes for each wine and the question is asking about names and scores of wine based on certain criteria.",
                "The question mentions \"names\", and the table contains names information in the \"name\" column.",
                "The question mentions \"scores\", and the table contains score information in the \"score\" column.",
                "The question mentions \"white color grapes\", and the table contains grapes information in the \"grape\" column.",
            ],
            "answer": "wine.score, wine.name, wine.grape",
        },
        {
            "context": "\n  Table Name: Reservations\n  This table has the following columns :\n     1. Code\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. Room\n        This column is of type TEXT and is nullable.\n     3. CheckIn\n        This column is of type TEXT and is nullable.\n     4. CheckOut\n        This column is of type TEXT and is nullable.\n     5. Rate\n        This column is of type REAL and is nullable.\n     6. LastName\n        This column is of type TEXT and is nullable.\n     7. FirstName\n        This column is of type TEXT and is nullable.\n     8. Adults\n        This column is of type INTEGER and is nullable.\n     9. Kids\n        This column is of type INTEGER and is nullable.\n",
            "question": "Which room has the highest rate? List the room's full name, rate, check in and check out date.",
            "thoughts": [
                "The table seems to describe reservation details of a hotel for each booking and the question is asking about details of room with the highest rate.",
                "The question mentions \"rate\", and the table contains room rate information in the \"rate\" column.",
                "The question mentions \"room's full name\", and the table contains room name information in the \"room\" column.",
                "The question mentions \"check in\", and the table contains check in information in the \"checkin\" column.",
                "The question mentions \"check out date\", and the table contains check out information in the \"checkout\" column.",
            ],
            "answer": "reservations.checkout, reservations.rate, reservations.room, reservations.checkin",
        },
        {
            "context": "\n  Table Name: Staff\n  This table has the following columns :\n     1. staff_id\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. staff_address_id\n        This column is of type INTEGER and is Non nullable.\n     3. nickname\n        This column is of type VARCHAR(80) and is nullable.\n     4. first_name\n        This column is of type VARCHAR(80) and is nullable.\n     5. middle_name\n        This column is of type VARCHAR(80) and is nullable.\n     6. last_name\n        This column is of type VARCHAR(80) and is nullable.\n     7. date_of_birth\n        This column is of type DATETIME and is nullable.\n     8. date_joined_staff\n        This column is of type DATETIME and is nullable.\n     9. date_left_staff\n        This column is of type DATETIME and is nullable.\n",
            "question": "Which country and state does staff with first name as Janessa and last name as Sawayn lived?",
            "thoughts": [
                "The table seems to describe staff details for each staff, and the question is asking about country and state details based on certain conditions.",
                "The question mentions \"country and state\", and the table contains address information in the \"address_id\" column.",
                "The question mentions \"staff with first name\", and the table contains first name information in the \"first_name\" column.",
                "The question mentions \"last name\", and the table contains last name information in the \"last_name\" column.",
            ],
            "answer": "staff.first_name, staff.last_name, staff.staff_address_id",
        },
        {
            "context": "\n  Table Name: Problems\n  This table has the following columns :\n     1. problem_id\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. product_id\n        This column is of type INTEGER and is Non nullable.\n     3. closure_authorised_by_staff_id\n        This column is of type INTEGER and is Non nullable.\n     4. reported_by_staff_id\n        This column is of type INTEGER and is Non nullable.\n     5. date_problem_reported\n        This column is of type DATETIME and is Non nullable.\n     6. date_problem_closed\n        This column is of type DATETIME and is nullable.\n     7. problem_description\n        This column is of type VARCHAR(255) and is nullable.\n     8. other_problem_details\n        This column is of type VARCHAR(255) and is nullable.\n",
            "question": "What are the product ids for the problems reported by Christop Berge with closure authorised by Ashley Medhurst?",
            "thoughts": [
                "The table seems to describe information about reported problems for each product, and the question is asking about product IDs for the problems reported and closed by certain people.",
                "The question mentions \"product ids\", and the table contains product id information in the \"product_id\" column.",
                "The question mentions \"reported by\", and the table contains reporting person information in the \"reported_by_staff_id\" column.",
                "The question mentions \"closure authorised by\", and the table contains closure authorization information in the \"closure_authorised_by_staff_id\" column.",
            ],
            "answer": "problems.closure_authorised_by_staff_id, problems.product_id, problems.reported_by_staff_id",
        },
        {
            "context": "\n  Table Name: airport\n  This table has the following columns :\n     1. Airport_ID\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. Airport_Name\n        This column is of type TEXT and is nullable.\n     3. Total_Passengers\n        This column is of type REAL and is nullable.\n     4. %_Change_2007\n        This column is of type TEXT and is nullable.\n     5. International_Passengers\n        This column is of type REAL and is nullable.\n     6. Domestic_Passengers\n        This column is of type REAL and is nullable.\n     7. Transit_Passengers\n        This column is of type REAL and is nullable.\n     8. Aircraft_Movements\n        This column is of type REAL and is nullable.\n     9. Freight_Metric_Tonnes\n        This column is of type REAL and is nullable.\n",
            "question": "Show all information on the airport that has the largest number of international passengers.",
            "thoughts": [
                "The table seems to describe various informations related to airport and passenger details for each airport, and the question is asking about information about the airport based on a criteria on international passengers",
                "The question mentions \"all information on airport\", and the table contains airport information in the \"aircraft_movements\", \"domestic_passengers\", \"airport_name\", \"freight_metric_tonnes\", \"transit_passengers\", \"_change_2007\",  \"airport_id\" columns.",
                "The question mentions criteria \"international passengers\" , and the table contains international passengers information in \"International_Passengers\" column",
            ],
            "answer": "airport.aircraft_movements, airport.domestic_passengers, airport.airport_name, airport.international_passengers, airport.freight_metric_tonnes, airport.transit_passengers, airport.%_change_2007, airport.total_passengers, airport.airport_id",
        },
        {
            "context": "\n  Table Name: company\n  This table has the following columns :\n     1. Company_ID\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. Rank\n        This column is of type INTEGER and is nullable.\n     3. Company\n        This column is of type TEXT and is nullable.\n     4. Headquarters\n        This column is of type TEXT and is nullable.\n     5. Main_Industry\n        This column is of type TEXT and is nullable.\n     6. Sales_billion\n        This column is of type REAL and is nullable.\n     7. Profits_billion\n        This column is of type REAL and is nullable.\n     8. Assets_billion\n        This column is of type REAL and is nullable.\n     9. Market_Value\n        This column is of type REAL and is nullable.\n",
            "question": "find the rank, company names, market values of the companies in the banking industry order by their sales and profits in billion.",
            "thoughts": [
                "The table seems to describe the details of each company and, the question is asking about details of the company in certain order and under certain filters.",
                "The question mentions \"rank\", and the table contains rank information in the \"rank\" column.",
                "The question mentions \"company names\", and the table contains company name information in the \"company\" column.",
                "The question mentions \"market values\", and the table contains market value information in the \"market_value\" column.",
                "The question mentions \"banking industry\", and the table contains banking industry information in the \"main_industry\" column.",
                "The question mentions \"order by sales\", and the table contains sales information in the \"sales_billion\" column.",
                "The question mentions \"profits in billion\", and the table contains profit information in the \"profits_billion\" column.",
            ],
            "answer": "company.profits_billion, company.market_value, company.sales_billion, company.rank, company.company, company.main_industry",
        },
        {
            "context": "\n  Table Name: basketball_match\n  This table has the following columns :\n     1. Team_ID\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. School_ID\n        This column is of type INTEGER and is nullable.\n     3. Team_Name\n        This column is of type TEXT and is nullable.\n     4. ACC_Regular_Season\n        This column is of type TEXT and is nullable.\n     5. ACC_Percent\n        This column is of type TEXT and is nullable.\n     6. ACC_Home\n        This column is of type TEXT and is nullable.\n     7. ACC_Road\n        This column is of type TEXT and is nullable.\n     8. All_Games\n        This column is of type TEXT and is nullable.\n     9. All_Games_Percent\n        This column is of type INTEGER and is nullable.\n     10. All_Home\n        This column is of type TEXT and is nullable.\n     11. All_Road\n        This column is of type TEXT and is nullable.\n     12. All_Neutral\n        This column is of type TEXT and is nullable.\n",
            "question": "What is the team name and acc regular season score of the school that was founded for the longest time?",
            "thoughts": [
                "The table seems to describe various basketball match details for each team and school, and the question is asking about team names and scores for schools filtered by certain criteria.",
                "The question mentions \"team name\", and the table contains team name information in the \"team_name\" column.",
                "The question mentions \"acc regular season score\", and the table contains acc regular season score information in the \"acc_regular_season\" column.",
                "The question mentions \"school info\", and the table contains school info in the column \"school_id\".",
                "The question mentions \"school that was founded for the longest time?\", but the table does not contain any information about founding date.",
            ],
            "answer": "basketball_match.acc_regular_season, basketball_match.school_id, basketball_match.team_name",
        },
        {
            "context": "\n  Table Name: employees\n  This table has the following columns :\n     1. id\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. last_name\n        This column is of type VARCHAR(20) and is Non nullable.\n     3. first_name\n        This column is of type VARCHAR(20) and is Non nullable.\n     4. title\n        This column is of type VARCHAR(30) and is nullable.\n     5. reports_to\n        This column is of type INTEGER and is nullable.\n     6. birth_date\n        This column is of type TIMESTAMP and is nullable.\n     7. hire_date\n        This column is of type TIMESTAMP and is nullable.\n     8. address\n        This column is of type VARCHAR(70) and is nullable.\n     9. city\n        This column is of type VARCHAR(40) and is nullable.\n     10. state\n        This column is of type VARCHAR(40) and is nullable.\n     11. country\n        This column is of type VARCHAR(40) and is nullable.\n     12. postal_code\n        This column is of type VARCHAR(10) and is nullable.\n     13. phone\n        This column is of type VARCHAR(24) and is nullable.\n     14. fax\n        This column is of type VARCHAR(24) and is nullable.\n     15. email\n        This column is of type VARCHAR(60) and is nullable.\n",
            "question": "How many customers does Steve Johnson support?",
            "thoughts": [
                "The table seems to describe employee details and company information for each employee, and the question is asking about customer count based on certain criteria.",
                "The question mentions \"how many customers\", and the table contains ID information in the \"id\" column.",
                "The question mentions \"Steve Johnson support\", and the table contains first name information in the \"first_name\" column.",
                "The question mentions \"Steve Johnson support\", and the table contains last name information in the \"last_name\" column.",
            ],
            "answer": "employees.first_name, employees.id, employees.last_name",
        },
        {
            "context": "\n  Table Name: Customer\n  This table has the following columns :\n     1. CustomerId\n        This column is of type INTEGER and is Non nullable.\n        This column is the primary key for this table\n     2. FirstName\n        This column is of type VARCHAR(40) and is Non nullable.\n     3. LastName\n        This column is of type VARCHAR(20) and is Non nullable.\n     4. Company\n        This column is of type VARCHAR(80) and is nullable.\n     5. Address\n        This column is of type VARCHAR(70) and is nullable.\n     6. City\n        This column is of type VARCHAR(40) and is nullable.\n     7. State\n        This column is of type VARCHAR(40) and is nullable.\n     8. Country\n        This column is of type VARCHAR(40) and is nullable.\n     9. PostalCode\n        This column is of type VARCHAR(10) and is nullable.\n     10. Phone\n        This column is of type VARCHAR(24) and is nullable.\n     11. Fax\n        This column is of type VARCHAR(24) and is nullable.\n     12. Email\n        This column is of type VARCHAR(60) and is Non nullable.\n     13. SupportRepId\n        This column is of type INTEGER and is nullable.\n",
            "question": "Find all invoice dates corresponding to customers with first name Astrid and last name Gruber.",
            "thoughts": [
                "The table seems to describe various customer information including personal and company details for each customer, and the question is asking about invoice dates for customer having certain names.",
                "The question mentions \"invoice date\", and the table does not contains information about invoice date.",
                "The question mentions criteria for \"first name\", and the table contains first name information in the \"firstname\" column.",
                "The question mentions criteria for \"last name\", and the table contains last name information in the \"lastname\" column.",
                "The question mentions \"customer\" information, and the table contains customer information in the \"customerid\" column",
            ],
            "answer": "customer.firstname, customer.customerid, customer.lastname",
        },
        {
            "context": "\n  Table Name: player\n  This table has the following columns :\n     1. player_id\n        This column is of type TEXT and is nullable.\n     2. birth_year\n        This column is of type NUMERIC and is nullable.\n     3. birth_month\n        This column is of type NUMERIC and is nullable.\n     4. birth_day\n        This column is of type NUMERIC and is nullable.\n     5. birth_country\n        This column is of type TEXT and is nullable.\n     6. birth_state\n        This column is of type TEXT and is nullable.\n     7. birth_city\n        This column is of type TEXT and is nullable.\n     8. death_year\n        This column is of type NUMERIC and is nullable.\n     9. death_month\n        This column is of type NUMERIC and is nullable.\n     10. death_day\n        This column is of type NUMERIC and is nullable.\n     11. death_country\n        This column is of type TEXT and is nullable.\n     12. death_state\n        This column is of type TEXT and is nullable.\n     13. death_city\n        This column is of type TEXT and is nullable.\n     14. name_first\n        This column is of type TEXT and is nullable.\n     15. name_last\n        This column is of type TEXT and is nullable.\n     16. name_given\n        This column is of type TEXT and is nullable.\n     17. weight\n        This column is of type NUMERIC and is nullable.\n     18. height\n        This column is of type NUMERIC and is nullable.\n     19. bats\n        This column is of type TEXT and is nullable.\n     20. throws\n        This column is of type TEXT and is nullable.\n     21. debut\n        This column is of type TEXT and is nullable.\n     22. final_game\n        This column is of type TEXT and is nullable.\n     23. retro_id\n        This column is of type TEXT and is nullable.\n     24. bbref_id\n        This column is of type TEXT and is nullable.\n",
            "question": "List players' first name and last name who have weight greater than 220 or height shorter than 75.",
            "thoughts": [
                "The table seems to contain information about each player, including their personal details, physical attributes, and career information, and the question is asking the names of the players with certain height and weight constraints.",
                "The question mentions \"player\u2019s first name\", and the table contains player\u2019s first name information in the \"name_first\" column.",
                "The question mentions \"player\u2019s last name\", and the table contains player\u2019s last name information in the \"name_last\" column.",
                "The question mentions \"weight\", and the table contains player\u2019s weight information in the \"weight\" column.",
                "The question mentions \"height\", and the table contains player\u2019s height information in the \"height\" column.",
            ],
            "answer": "player.weight, player.name_first, player.name_last, player.height",
        },
        {
            "context": "\n  Table Name: Catalog_Contents\n  This table has the following columns :\n     1. catalog_entry_id\n        This column is of type INTEGER and is nullable.\n        This column is the primary key for this table\n     2. catalog_level_number\n        This column is of type INTEGER and is Non nullable.\n     3. parent_entry_id\n        This column is of type INTEGER and is nullable.\n     4. previous_entry_id\n        This column is of type INTEGER and is nullable.\n     5. next_entry_id\n        This column is of type INTEGER and is nullable.\n     6. catalog_entry_name\n        This column is of type VARCHAR(80) and is nullable.\n     7. product_stock_number\n        This column is of type VARCHAR(50) and is nullable.\n     8. price_in_dollars\n        This column is of type DOUBLE and is nullable.\n     9. price_in_euros\n        This column is of type DOUBLE and is nullable.\n     10. price_in_pounds\n        This column is of type DOUBLE and is nullable.\n     11. capacity\n        This column is of type VARCHAR(20) and is nullable.\n     12. length\n        This column is of type VARCHAR(20) and is nullable.\n     13. height\n        This column is of type VARCHAR(20) and is nullable.\n     14. width\n        This column is of type VARCHAR(20) and is nullable.\n",
            "question": "What are the entry names of catalog with the attribute possessed by most entries.",
            "thoughts": [
                "The table seems to describe various details of products for each catalog, and the question is asking about catalog entry names under specific conditions",
                "The question mentions \"entry names\", and the table contains entry names information in the \"catalog_entry_name\" column.",
                "The question mentions filtering by \"most entries\", and the table contains entry information in the \"catalog_entry_id\" column.",
            ],
            "answer": "catalog_contents.catalog_entry_id, catalog_contents.catalog_entry_name",
        },
    ]

