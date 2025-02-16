#!/usr/bin/env python
"""
judge_agent.py
Implements the Judge Agent that validates outputs from various agents.
Perform basic validation checks and returns validation messages.
"""

def validate_output(output_type, output_data):
    if output_type == "health":
        if output_data.get("heart_rate", 0) > 100:
            return False, "Heart rate too high; please re-run health analysis."
    elif output_type == "report":
        if "report" not in output_data.lower():
            return False, "Generated report invalid; please regenerate."
    return True, "Output validated successfully."

if __name__ == "__main__":
    dummy_health = {"heart_rate": 120}
    valid, message = validate_output("health", dummy_health)
    print("Health Validation:", message)
