{"vegasTransactionId":"787b76d5-1861-482f-9cd8-da151f618717","errorCode":"424 FAILED_DEPENDENCY","message":"400 Bad Request: \"[{<EOL>  \"error\": {<EOL>    \"code\": 400,<EOL>    \"message\": \"The specified schema produces a constraint that has too many states for serving.  Typical causes of this error are schemas with lots of text (for example, very long property or enum names), schemas with long array length limits (especially when nested), or schemas using complex value matchers (for example, integers or numbers with minimum/maximum bounds or strings with complex formats like date-time)\",<EOL>    \"status\": \"INVALID_ARGUMENT\"<EOL>  }<EOL>}<EOL>]\"","statusCode":424,"statusName":"FAILED_DEPENDENCY","path":"/vegas/apps/prompt/LLMInsight","method":"POST","timestamp":"2025-01-29T19:29:12.071815924"}

import requests
import json

url = "https://vegas-llm.verizon.com/vegas/apps/prompt/LLMInsight"

payload = json.dumps({
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
                    "type": "string",
                    "enum": [
                        "negative",
                        "positive",
                        "neutral"
                    ]
                },
                "topics": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "topic": {
                                "type": "string"
                            },
                            "confidence_score": {
                                "type": "integer"
                            },
                            "sentiment": {
                                "type": "string",
                                "enum": [
                                    "negative",
                                    "positive",
                                    "neutral"
                                ]
                            },
                            "relevant_phrases": {
                                "type": "string"
                            }
                        },
                        "required": [
                            "topic",
                            "confidence_score",
                            "sentiment",
                            "relevant_phrases"
                        ]
                    }
                },
                "intention": {
                    "type": "string"
                },
                "customer_satisfied": {
                    "type": "boolean"
                },
                "agent_resolution": {
                    "type": "string"
                },
                "agent_resolved_customer_concern": {
                    "type": "boolean"
                },
                "qna": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cust_question": {
                                "type": "string"
                            },
                            "agent_answer": {
                                "type": "string"
                            },
                            "is_cust_satisfied_with_answer": {
                                "type": "boolean"
                            },
                            "question_answered_properly": {
                                "type": "boolean"
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
                "customer_satisfied",
                "agent_resolution",
                "agent_resolved_customer_concern",
                "qna"
            ]
        }
    },
    "preSeed_injection_map": {
        "": "Who are you?"  # Consider removing if not needed
    }
})
headers = {
    'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
