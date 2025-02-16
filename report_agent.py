#!/usr/bin/env python
"""
report_agent.py
Uses Mistral API to generate a final report based on a given task description.
"""

import requests
import os

# Set your Mistral API key and endpoint
MISTRAL_API_KEY = "LDIPhUAb8kUwgmwzX88ADpT2tBXj8UY0"
MISTRAL_API_ENDPOINT = "https://api.mistral.ai/v1/generate_report"  # Update with actual endpoint

def generate_report(task_description):
    """
    Calls Mistral API to generate a report based on the task description.
    """
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "task": task_description
    }

    try:
        response = requests.post(MISTRAL_API_ENDPOINT, json=payload, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        return response.json().get("report", "No report generated.")
    except requests.exceptions.RequestException as e:
        return f"Error generating report: {e}"

if __name__ == "__main__":
    # Example usage of generate_report
    description = "Generate a mission readiness report for the squad."
    result = generate_report(description)
    print("Report Generation Result:", result)
