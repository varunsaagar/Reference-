import json
import requests
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from jsonschema import validate, ValidationError

# Embedded schema based on the provided specification
CONVERSATION_ANALYSIS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Conversation Analysis Schema",
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "Precise summary of the entire conversation without missing any details."
        },
        "sentiment": {
            "type": "string",
            "enum": ["negative", "positive", "neutral"],
            "description": "Overall sentiment of the customer, strictly one of: 'negative', 'positive', or 'neutral'."
        },
        "topics": {
            "type": "array",
            "description": "Accurately determined topics from the conversation.",
            "items": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Short phrase describing the topic (1-3 words)."
                    },
                    "confidence_score": {
                        "type": "number",
                        "description": "Confidence score for this topic (range 0-100)."
                    },
                    "sentiment": {
                        "type": "string",
                        "enum": ["negative", "positive", "neutral"],
                        "description": "Sentiment associated with this specific topic."
                    },
                    "relevant_phrases": {
                        "description": "Evidence sentence(s) or phrase(s) indicating why this topic was determined.",
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}}
                        ]
                    }
                },
                "required": ["topic", "confidence_score", "sentiment", "relevant_phrases"]
            }
        },
        "intention": {
            "type": "string",
            "description": "Overall intention of the conversation in strictly one to three words."
        },
        "confidence_score_overall": {
            "type": "number",
            "description": "Confidence (0-100) for the overall intention."
        },
        "customer_intention": {
            "type": "string",
            "description": "Customer's primary intention in two to three words."
        },
        "customer_intention_secondary": {
            "type": "string",
            "description": "Secondary intention if multiple issues or requests are discussed. Otherwise can be null or empty."
        },
        "confidence_score_customer_intention_secondary": {
            "type": "number",
            "description": "Confidence (0-100) for the secondary intention. Can be null or 0 if no secondary intention."
        },
        "customer_satisfied": {
            "type": "string",
            "enum": ["Yes", "No"],
            "description": "Indicates if customer is satisfied with agent resolution at the end of call."
        },
        "agent_resolution": {
            "type": "string",
            "description": "Detailed summary of agent actions and the resolution or help provided."
        },
        "agent_resolved_customer_concern": {
            "type": "string",
            "enum": ["yes", "no"],
            "description": "Whether the agent successfully resolved the customer's concern."
        },
        "supervisor_escalation": {
            "type": "string",
            "enum": ["yes", "no"],
            "description": "If customer requested or agent had to escalate to a supervisor."
        },
        "emotion": {
            "type": "string",
            "enum": ["frustrated", "angry", "none"],
            "description": "If the customer was not satisfied, specify if they're frustrated or angry. 'none' if not applicable."
        },
        "callback_promied": {
            "type": "string",
            "enum": ["yes", "no"],
            "description": "Indicates if the agent promised a callback for further action or resolution."
        },
        "call_disconnection": {
            "type": "string",
            "enum": ["yes", "no"],
            "description": "If the call was disconnected or abandoned."
        },
        "troubleshoot_ticket": {
            "type": "string",
            "enum": ["yes", "no"],
            "description": "If a troubleshoot ticket was created (or offered) due to unsatisfactory resolution."
        },
        "customer_issue": {
            "type": "string",
            "description": "Accurate statement of the customer's issue or concern in detail."
        },
        "reason_for_disconnect": {
            "type": "string",
            "description": "Brief reason why customer wants to disconnect, or 'unable to determine' if not clear."
        },
        "repeat_call_reason": {
            "type": "string",
            "description": "If customer has called earlier for the same issue, specify how many times and relevant evidence from transcript."
        }
    },
    "required": [
        "summary",
        "sentiment",
        "topics",
        "intention",
        "confidence_score_overall",
        "customer_intention",
        "customer_intention_secondary",
        "confidence_score_customer_intention_secondary",
        "customer_satisfied",
        "agent_resolution",
        "agent_resolved_customer_concern",
        "supervisor_escalation",
        "callback_promied",
        "call_disconnection",
        "troubleshoot_ticket",
        "customer_issue",
        "reason_for_disconnect",
        "repeat_call_reason"
    ]
}

@dataclass
class VegasConfig:
    """Configuration class for Vegas API"""
    api_url: str
    usecase: str
    context_id: str
    temperature: float = 0.9
    max_output_tokens: int = 2048
    top_p: float = 1.0

class SchemaManager:
    """Manages schema validation"""
    
    def __init__(self):
        self.schema = CONVERSATION_ANALYSIS_SCHEMA
    
    def validate_response(self, response: Dict[str, Any]) -> None:
        """Validate response against the embedded schema"""
        try:
            validate(instance=response, schema=self.schema)
        except ValidationError as e:
            raise ValidationError(f"Response validation failed: {str(e)}") from e
    
    def get_schema(self) -> Dict[str, Any]:
        """Return the embedded schema"""
        return self.schema

class VegasAPI:
    """Handler for Vegas LLM API interactions"""
    
    def __init__(self, config: VegasConfig):
        self.config = config
        self.schema_manager = SchemaManager()
        self.headers = {"Content-Type": "application/json"}
    
    def prepare_payload(self, input_text: str) -> Dict[str, Any]:
        """Prepare API payload with embedded schema"""
        return {
            "useCase": self.config.usecase,
            "contextId": self.config.context_id,
            "parameters": {
                "temperature": self.config.temperature,
                "maxOutputTokens": self.config.max_output_tokens,
                "topP": self.config.top_p,
                "responseMimeType": "application/json",
                "responseSchema": self.schema_manager.get_schema()
            },
            "preSeed_injection_map": {
                "": input_text
            }
        }
    
    def call_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make API call and validate response"""
        try:
            response = requests.post(
                self.config.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            response_data = response.json()
            self.schema_manager.validate_response(response_data)
            return response_data
        except requests.exceptions.RequestException as e:
            raise Exception(f"API call failed: {str(e)}") from e

class VegasLLMHandler:
    """Main handler class for Vegas LLM operations"""
    
    def __init__(self, config_path: str):
        """Initialize handler with config and embedded schema"""
        self.config = self._load_config(config_path)
        self.vegas_api = VegasAPI(self.config)
    
    @staticmethod
    def _load_config(config_path: str) -> VegasConfig:
        """Load configuration from file"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            return VegasConfig(**config_data)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Config file not found at {config_path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON config in file {config_path}") from e
    
    def process_input(self, input_text: str) -> Dict[str, Any]:
        """Process input through Vegas LLM"""
        payload = self.vegas_api.prepare_payload(input_text)
        return self.vegas_api.call_api(payload)

def main():
    """Main execution function"""
    config_path = Path("config/vegas_config.json")
    
    try:
        handler = VegasLLMHandler(str(config_path))
        input_text = "Sample conversation transcript here..."
        response = handler.process_input(input_text)
        print(json.dumps(response, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
