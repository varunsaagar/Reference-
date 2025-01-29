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
            "type": "OBJECT",  # Added type here
            "properties": {
                "summary": {
                    "type": "STRING"
                },
                "sentiment": {
                    "type": "STRING",
                    "enum": [
                        "negative",
                        "positive",
                        "neutral"
                    ]
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
                                "enum": [
                                    "negative",
                                    "positive",
                                    "neutral"
                                ]
                            },
                            "relevant_phrases": {
                                "type": "ARRAY", #added type array 
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
                        "required": [
                            "topic",
                            "confidence_score",
                            "sentiment",
                            "relevant_phrases"
                        ]
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
                    "enum": [
                        "Yes",
                        "No"
                    ]
                },
                "agent_resolution": {
                    "type": "STRING"
                },
                "agent_resolved_customer_concern": {
                    "type": "STRING",
                    "enum": [
                        "yes",
                        "no"
                    ]
                },
                "supervisor_escalation": {
                    "type": "STRING",
                    "enum": [
                        "yes",
                        "no"
                    ]
                },
                "emotion": {
                    "type": "STRING",
                    "enum": [
                        "frustrated",
                        "angry",
                        "none"
                    ]
                },
                "callback_promied": {
                    "type": "STRING",
                    "enum": [
                        "yes",
                        "no"
                    ]
                },
                "call_disconnection": {
                    "type": "STRING",
                    "enum": [
                        "yes",
                        "no"
                    ]
                },
                "troubleshoot_ticket": {
                    "type": "STRING",
                    "enum": [
                        "yes",
                        "no"
                    ]
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
                                "enum": [
                                    "yes",
                                    "no",
                                    "partial",
                                    "unable_to_determine"
                                ]
                            },
                            "question_answered_properly": {
                                "type": "STRING",
                                "enum": [
                                    "yes",
                                    "no",
                                    "partial",
                                    "unable_to_determine"
                                ]
                            }
                        },
                        "required": [
                            "cust_question",
                            "agent_answer",
                            "is_cust_satisfied_with_answer",
                            "question_answered_properly"
                        ]
                    }
                },
                "customer_res_reason": {
                    "type": "STRING"
                },
                "start_of_call_sentiment": {
                    "type": "STRING"
                },
                "start_of_call_intent": {
                    "type": "STRING"
                },
                "start_of_call_intent_agent_resolution": {
                    "type": "STRING"
                },
                "customer_satisfied_for_agent_res_starting_intent": {
                    "type": "STRING"
                },
                "start_of_call_customer_frustration_level": {
                    "type": "STRING"
                },
                "start_of_call_customer_anger_level": {
                    "type": "STRING"
                },
                "start_of_call_cust_frustration_after_agent_resolution": {
                    "type": "STRING"
                },
                "start_of_call_cust_sentiment_after_agent_resolution": {
                    "type": "STRING"
                },
                "end_of_call_sentiment": {
                    "type": "STRING"
                },
                "end_of_call_intent": {
                    "type": "STRING"
                },
                "end_of_call_intent_agent_resolution": {
                    "type": "STRING"
                },
                "customer_satisfied_for_agent_res_ending_intent": {
                    "type": "STRING"
                },
                "end_of_call_customer_frustration_level": {
                    "type": "STRING"
                },
                "end_of_call_customer_anger_level": {
                    "type": "STRING"
                },
                "end_of_call_cust_frustration_after_agent_resolution": {
                    "type": "STRING"
                },
                "end_of_call_cust_sentiment_after_agent_resolution": {
                    "type": "STRING"
                },
                "possible_churn_reason": {
                    "type": "STRING"
                },
                "customer_unsatisfaction_phrase": {
                    "type": "STRING"
                },
                "customer_unsatisfaction_phrase_intent": {
                    "type": "STRING"
                },
                "device_name": {
                    "type": "STRING"
                },
                "feature_name": {
                    "type": "STRING"
                },
                "confidence_score_customer_intention_primary": {
                    "type": "STRING"
                },
                 "resolution_emotion": {
                    "type": "STRING"
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
                "qna",
                "customer_res_reason",
                "start_of_call_sentiment",
                "start_of_call_intent",
                "start_of_call_intent_agent_resolution",
                "customer_satisfied_for_agent_res_starting_intent",
                "start_of_call_customer_frustration_level",
                "start_of_call_customer_anger_level",
                "start_of_call_cust_frustration_after_agent_resolution",
                "start_of_call_cust_sentiment_after_agent_resolution",
                "end_of_call_sentiment",
                "end_of_call_intent",
                "end_of_call_intent_agent_resolution",
                "customer_satisfied_for_agent_res_ending_intent",
                "end_of_call_customer_frustration_level",
                "end_of_call_customer_anger_level",
                "end_of_call_cust_frustration_after_agent_resolution",
                "end_of_call_cust_sentiment_after_agent_resolution",
                "possible_churn_reason",
                "customer_unsatisfaction_phrase",
                "customer_unsatisfaction_phrase_intent",
                "device_name",
                "feature_name",
                "confidence_score_customer_intention_primary",
                "resolution_emotion"

            ]
        }
    },
    "preSeed_injection_map": {
        "": "Who are you?"
    }
})
headers = {
    'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
