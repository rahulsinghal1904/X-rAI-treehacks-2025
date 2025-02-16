#!/usr/bin/env python
"""
judge_agent.py
Contains the Judge Agent that validates outputs from various agents.
Performs simple threshold checks and triggers course correction if needed.
"""

def validate_output(output_type, output_data):
    # For demonstration, we simulate simple validation rules.
    if output_type == "health":
        # Validate health output: if heart rate above 100, consider it an error.
        if output_data.get("heart_rate", 0) > 100:
            return False, "Heart rate too high; re-run health analysis."
    elif output_type == "report":
        # Validate report output: must contain the word "report"
        if "report" not in output_data.lower():
            return False, "Generated report appears invalid; regenerate."
    # Assume market and meeting outputs are always valid for demo.
    return True, None

if __name__ == "__main__":
    # Example usage: simulate a health validation check.
    dummy_health = {"heart_rate": 120}
    valid, message = validate_output("health", dummy_health)
    if not valid:
        print("Validation Error:", message)
    else:
        print("Output validated successfully.")
