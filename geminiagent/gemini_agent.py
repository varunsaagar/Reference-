# gemini_agent.py
import vertexai
from vertexai.generative_models import (
    FunctionDeclaration,
    GenerativeModel,
    Part,
    Tool,
)
from data_access import BigQueryManager
from typing import List, Dict, Tuple
import re

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

        # NLU Components (using regex for simplicity, can be replaced with spaCy/Transformers)
        self.intents = {
            "get_call_metrics": r"\b(metrics|duration|handle time|hold time|talk time|abandons|count)\b",
            "get_agent_performance": r"\b(agent|rep|representative)\b",
            "get_customer_info": r"\b(customer|caller|client)\b",
            "get_call_details": r"\b(call details|specific calls)\b",
            "get_transfer_info": r"\b(transfer|transferred)\b",
            "get_abandon_info": r"\b(abandon|abandoned)\b",
        }

        self.entities = {
            "DATE_RANGE": r"\b(last week|yesterday|today|this week|this month|last month|\d{4}-\d{2}-\d{2})\b",
            "TIME": r"\b([01]?[0-9]|2[0-3]):[0-5][0-9]\b",  # Simple HH:MM format
            "METRIC": r"\b(call duration|handle time|hold time|talk time|ring time|delay time|abandon rate|call count)\b",
            "TOPIC": r"\b(billing|technical support|sales|account|payment)\b",
            "AGENT_NAME": r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b",  # Simple pattern for names
            "CUSTOMER_SEGMENT": r"\b(prepaid|postpaid)\b",
            "TRANSFER_STATUS": r"\b(transferred)\b",
            "CALL_DISPOSITION": r"\b(answered|abandoned)\b",
            "BUSINESS_UNIT": r"\b(eccr_line_bus_nm|eccr_super_line_bus_nm)\b",
            "CALL_CENTER": r"\b(eccr_call_ctr_cd)\b",
            "PHONE_NUMBER": r"\b(\d{3}-\d{3}-\d{4})\b",  # Simple XXX-XXX-XXXX format
            "REGION": r"\b(callers_region)\b",
            "BUSINESS_RULE": r"\b(business rule)\b",
            "SUPER_BUSINESS_RULE": r"\b(super business rule)\b",
            "SUPER_SKILL_GROUP": r"\b(super skill group)\b",
            "SUPER_CALL_TYPE": r"\b(super call type)\b",
        }

        self.entity_column_map = {
            "METRIC": {
                "call duration": "call_duration_seconds",
                "handle time": "handle_tm_seconds",
                "hold time": "hold_tm_seconds",
                "talk time": "talk_tm_seconds",
                "ring time": "ring_tm_seconds",
                "delay time": "delay_tm_seconds",
                "abandon rate": "abandons_cnt",
                "call count": "answered_cnt"
            },
            "DATE_RANGE": ["call_end_dt", "call_answer_dt"],
            "TOPIC": ["acd_area_nm", "script_nm", "eccr_dept_nm", "bus_rule", "super_bus_rule"],
            "CUSTOMER_SEGMENT": ["icm_acct_type_cd", "cust_value"],
            "AGENT_NAME/ID": [],  # Needs external lookup
            "TRANSFER_STATUS": ["transfer_flag", "transfer_point"],
            "CALL_DISPOSITION": ["final_call_dispo", "call_dispo_flag", "abandons_cnt", "answered_cnt"],
            "BUSINESS_UNIT": ["eccr_line_bus_nm", "eccr_super_line_bus_nm"],
            "CALL_CENTER": ["eccr_call_ctr_cd"],
            "PHONE_NUMBER": ["mtn"],
            "REGION": ["callers_region"],
            "BUSINESS_RULE": ["bus_rule"],
            "SUPER_BUSINESS_RULE": ["super_bus_rule"],
            "SUPER_SKILL_GROUP": ["super_skill_group"],
            "SUPER_CALL_TYPE": ["super_call_type"]
        }
        
        # Define the function declarations
        self.get_table_schema_func = FunctionDeclaration(
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

        self.execute_sql_query_func = FunctionDeclaration(
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

        # Create the tool that includes the function declarations
        self.bq_tool = Tool(
            function_declarations=[
                self.get_table_schema_func,
                self.execute_sql_query_func,
            ],
        )

        # Initialize the Gemini model
        self.model = GenerativeModel(
            "gemini-1.5-pro-002",
            tools=[self.bq_tool],
            generation_config={"temperature": 0},
        )

        self.chat = self.model.start_chat()

    def _extract_intent(self, user_query: str) -> str:
        """Extracts the intent from the user query using regex."""
        for intent, pattern in self.intents.items():
            if re.findall(pattern, user_query, re.IGNORECASE):
                return intent
        return "general_query"  # Default intent

    def _extract_entities(self, user_query: str) -> Dict[str, List[str]]:
        """Extracts entities from the user query using regex."""
        extracted_entities = {}
        for entity_type, pattern in self.entities.items():
            matches = re.findall(pattern, user_query, re.IGNORECASE)
            if matches:
                extracted_entities[entity_type] = matches
        return extracted_entities
    
    def _map_entities_to_columns(self, extracted_entities):
        """Maps extracted entities to their corresponding database columns."""
        entity_column_mapping = {}
        for entity_type, entities in extracted_entities.items():
            if entity_type in self.entity_column_map:
                if entity_type == "METRIC":
                    # Handle metric mapping to columns
                    for entity in entities:
                        if entity.lower() in self.entity_column_map[entity_type]:
                            column_name = self.entity_column_map[entity_type][entity.lower()]
                            if entity_type not in entity_column_mapping:
                                entity_column_mapping[entity_type] = []
                            entity_column_mapping[entity_type].append(column_name)
                else:
                    # Direct mapping for other entity types
                    entity_column_mapping[entity_type] = self.entity_column_map[entity_type]
        return entity_column_mapping

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

    def _generate_sql_prompt(self, user_query: str, intent: str, entity_mapping: Dict[str, List[str]]) -> str:
        """Generates a prompt for the Gemini model to generate a SQL query."""

        # Basic prompt
        prompt = f"""
        You are a helpful assistant that can convert natural language into SQL queries for Bigquery.

        You have access to the following BigQuery table:
        {self.formatted_table_schema}

        Convert the following natural language query into a SQL query:
        {user_query}
        """
        
        # Add intent information
        prompt += f"\nIdentified intent: {intent}"

        # Add entity-column mapping information
        if entity_mapping:
            prompt += "\n\nRelevant entities and their mappings to columns:"
            for entity_type, columns in entity_mapping.items():
                if entity_type == "METRIC":
                    # Special handling for metrics
                    for metric, column in zip(entity_mapping["METRIC"], columns):
                        prompt += f"\n- {metric} -> {column}"
                else:
                    # General case for other entities
                    for column in columns:
                        prompt += f"\n- {entity_type} -> {column}"



        # Add instructions for SQL generation
        prompt += """
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

        """

        # Add few-shot examples based on intent (can be expanded)
        if intent == "get_call_metrics":
            prompt += """
            Examples:
            Question: How many calls were abandoned yesterday?
            SQL: SELECT COUNT(*) FROM `icm_summary_fact_exp` WHERE DATE(call_end_dt) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND abandons_cnt > 0

            Question: What is the average handle time for calls from the 'billing' department last week?
            SQL: SELECT AVG(handle_tm_seconds) FROM `icm_summary_fact_exp` WHERE eccr_dept_nm = 'billing' AND call_end_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            """

        return prompt

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
        else:
            raise ValueError(f"Unknown function: {function_name}")

        return function_response

    def process_query(self, user_query: str) -> str:
        """Processes the user query using the Gemini model and function calling."""
        # 1. Perform basic NLU
        intent = self._extract_intent(user_query)
        extracted_entities = self._extract_entities(user_query)
        entity_mapping = self._map_entities_to_columns(extracted_entities)

        # 2. Generate SQL prompt
        sql_prompt = self._generate_sql_prompt(user_query, intent, entity_mapping)

        # 3. Send prompt to Gemini and handle function calls
        response = self.chat.send_message(sql_prompt)
        print(f"Initial response: {response.candidates[0]}")

        # Handle function calls iteratively
        while response.candidates[0].finish_reason == "TOOL":
            print(f"Function called in loop : {response.candidates[0].finish_reason}")
            function_response = self._handle_function_call(response.candidates[0].content.parts[0])
            response = self.chat.send_message(function_response)

        try:
            # 4. Extract and return the final response
            final_response = response.candidates[0].content.parts[0].text
            print(f"Final response: {final_response}")
            return final_response
        except (AttributeError, IndexError) as e:
            error_message = f"Error processing response: {e}"
            print(error_message)
            return error_message