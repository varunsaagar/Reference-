import json
import requests
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from jsonschema import validate, ValidationError

# Embedded schema based on the provided specification
CONVERSATION_ANALYSIS_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "summary": {
            "type": "STRING"
        },
        "sentiment": {
            "type": "STRING",
            "enum": ["negative", "positive", "neutral"]
        },
        "topics": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "topic": {
                        "type": "STRING"
                    },
                    "confidence_score": {
                        "type": "INTEGER"
                    },
                    "sentiment": {
                        "type": "STRING",
                        "enum": ["negative", "positive", "neutral"]
                    },
                    "relevant_phrases": {
                        "oneOf": [
                            {
                                "type": "STRING"
                            },
                            {
                                "type": "ARRAY",
                                "items": {
                                    "type": "STRING"
                                }
                            }
                        ]
                    }
                },
                "required": ["topic", "confidence_score", "sentiment", "relevant_phrases"]
            }
        },
        "intention": {
            "type": "STRING"
        },
        "confidence_score_overall": {
            "type": "INTEGER"
        },
        "customer_intention": {
            "type": "STRING"
        },
        "customer_intention_secondary": {
            "type": "STRING",
            "nullable": true
        },
        "confidence_score_customer_intention_secondary": {
            "type": "INTEGER",
            "nullable": true
        },
        "customer_satisfied": {
            "type": "STRING",
            "enum": ["Yes", "No"]
        },
        "agent_resolution": {
            "type": "STRING"
        },
        "agent_resolved_customer_concern": {
            "type": "STRING",
            "enum": ["yes", "no"]
        },
        "supervisor_escalation": {
            "type": "STRING",
            "enum": ["yes", "no"]
        },
        "emotion": {
            "type": "STRING",
            "enum": ["frustrated", "angry", "none"]
        },
        "callback_promied": {
            "type": "STRING",
            "enum": ["yes", "no"]
        },
        "call_disconnection": {
            "type": "STRING",
            "enum": ["yes", "no"]
        },
        "troubleshoot_ticket": {
            "type": "STRING",
            "enum": ["yes", "no"]
        },
        "customer_issue": {
            "type": "STRING"
        },
        "reason_for_disconnect": {
            "type": "STRING"
        },
        "repeat_call_reason": {
            "type": "STRING"
        },
        "qna": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "cust_question": {
                        "type": "STRING"
                    },
                    "agent_answer": {
                        "type": "STRING"
                    },
                    "is_cust_satisfied_with_answer": {
                        "type": "STRING",
                        "enum": ["yes", "no", "partial", "unable_to_determine"]
                    },
                    "question_answered_properly": {
                        "type": "STRING",
                        "enum": ["yes", "no", "partial", "unable_to_determine"]
                    }
                },
                "required": [
                    "cust_question",
                    "agent_answer",
                    "is_cust_satisfied_with_answer",
                    "question_answered_properly"
                ]
            }
        }
    },
    "required": [
        "summary",
        "sentiment",
        "topics",
        "intention",
        "confidence_score_overall",
        "customer_intention",
        "customer_satisfied",
        "agent_resolution",
        "agent_resolved_customer_concern",
        "supervisor_escalation",
        "callback_promied",
        "call_disconnection",
        "troubleshoot_ticket",
        "customer_issue",
        "reason_for_disconnect",
        "repeat_call_reason",
        "qna"
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
