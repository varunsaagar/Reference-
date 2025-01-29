{
    "useCase": "convoiq_gemini",
    "contextId": "convoiq_exploratory_analysis_gem15pro",
    "parameters": {
        "temperature": 0.9,
        "maxOutputTokens": 2048,
        "topP": 1,
        "responseMimeType": "application/json",
        "responseSchema": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string"
                },
                "sentiment": {
                    "type": "string"
                }
            },
            "required": [
                "summary",
                "sentiment"
            ]
        }
    },
    "preSeed_injection_map": {
        "": "Who are you?"
    }
}


import json
import requests
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class VegasConfig:
    """Configuration class for Vegas API"""
    api_url: str
    usecase: str
    context_id: str
    temperature: float = 0.9
    max_output_tokens: int = 2048
    top_p: float = 1.0

class SchemaLoader:
    """Handles loading and validation of JSON schema files"""
    
    @staticmethod
    def load_schema(schema_path: str) -> Dict[str, Any]:
        """
        Load JSON schema from file
        
        Args:
            schema_path: Path to the JSON schema file
            
        Returns:
            Dict containing the loaded schema
        """
        try:
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            return schema
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found at {schema_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON schema in file {schema_path}")

class VegasAPI:
    """Handler for Vegas LLM API interactions"""
    
    def __init__(self, config: VegasConfig):
        """
        Initialize Vegas API handler
        
        Args:
            config: VegasConfig object containing API configuration
        """
        self.config = config
        self.headers = {"Content-Type": "application/json"}
    
    def prepare_payload(self, input_text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare the API payload
        
        Args:
            input_text: Input text for the model
            schema: JSON schema for response structuring
            
        Returns:
            Dict containing the formatted payload
        """
        return {
            "useCase": self.config.usecase,
            "contextId": self.config.context_id,
            "parameters": {
                "temperature": self.config.temperature,
                "maxOutputTokens": self.config.max_output_tokens,
                "topP": self.config.top_p,
                "responseMimeType": "json",
                "responseSchema": schema
            },
            "preSeed_injection_map": {
                "{INPUT}": input_text
            }
        }
    
    def call_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make the API call to Vegas
        
        Args:
            payload: Prepared API payload
            
        Returns:
            Dict containing the API response
        """
        try:
            response = requests.post(
                self.config.api_url,
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"API call failed: {str(e)}")

class VegasLLMHandler:
    """Main handler class for Vegas LLM operations"""
    
    def __init__(self, config_path: str, schema_path: str):
        """
        Initialize the Vegas LLM handler
        
        Args:
            config_path: Path to configuration file
            schema_path: Path to schema file
        """
        self.config = self._load_config(config_path)
        self.schema_loader = SchemaLoader()
        self.schema = self.schema_loader.load_schema(schema_path)
        self.vegas_api = VegasAPI(self.config)
    
    @staticmethod
    def _load_config(config_path: str) -> VegasConfig:
        """
        Load configuration from file
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            VegasConfig object
        """
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return VegasConfig(**config_data)
    
    def process_input(self, input_text: str) -> Dict[str, Any]:
        """
        Process input text through Vegas LLM
        
        Args:
            input_text: Input text to process
            
        Returns:
            Dict containing the processed response
        """
        payload = self.vegas_api.prepare_payload(input_text, self.schema)
        return self.vegas_api.call_api(payload)

def main():
    """Main execution function"""
    # Example usage
    config_path = "config/vegas_config.json"
    schema_path = "config/response_schema.json"
    
    try:
        handler = VegasLLMHandler(config_path, schema_path)
        
        # Example input text
        input_text = "Who are you?"
        
        # Process the input
        response = handler.process_input(input_text)
        
        # Print the response
        print(json.dumps(response, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
