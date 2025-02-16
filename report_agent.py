#!/usr/bin/env python
"""
report_agent.py
Uses Mistral API to generate a final report based on a given task description.
"""

import os
import requests

# Load API key from environment variable
MISTRAL_API_KEY = "LDIPhUAb8kUwgmwzX88ADpT2tBXj8UY0"
if not MISTRAL_API_KEY:
    raise ValueError("MISTRAL_API_KEY is not set. Please define it in environment variables.")

# Define the Mistral API endpoints
CHAT_API_ENDPOINT = "https://api.mistral.ai/v1/chat/completions"
CODE_API_ENDPOINT = "https://api.mistral.ai/v1/fim/completions"  # For code generation tasks

def generate_report(task_description, is_code_task=False):
    """
    Calls Mistral API to generate a report based on the task description.
    If is_code_task is True, it uses the Codestral model for code generation.
    """
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    if is_code_task:
        # Use Codestral model for code generation
        endpoint = CODE_API_ENDPOINT
        payload = {
            "model": "codestral-latest",
            "prompt": task_description,
            "max_tokens": 256,
            "temperature": 0.7
        }
    else:
        # Use general chat model for other tasks
        endpoint = CHAT_API_ENDPOINT
        payload = {
            "model": "mistral-large-latest",
            "messages": [{"role": "user", "content": task_description}],
            "temperature": 0.7
        }

    try:
        response = requests.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        if is_code_task:
            return response.json().get("choices", [{}])[0].get("text", "No code generated.")
        else:
            return response.json().get("choices", [{}])[0].get("message", {}).get("content", "No report generated.")
    except requests.exceptions.RequestException as e:
        return f"Error generating report: {e}"

if __name__ == "__main__":
    # Example usage of generate_report
    description = "Generate a mission readiness report for the squad."
    result = generate_report(description)
    print("Report Generation Result:", result)

    # Example usage for code generation
    code_description = "Write a Python function to calculate the Fibonacci sequence."
    code_result = generate_report(code_description, is_code_task=True)
    print("Code Generation Result:", code_result)
