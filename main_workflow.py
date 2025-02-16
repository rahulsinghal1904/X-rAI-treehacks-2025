#!/usr/bin/env python
"""
main_workflow.py
Integrates all agent modules into a unified workflow.
It parses the workflow, routes tasks to the appropriate agents, runs each agent,
and applies validation using the Judge Agent.
"""

from workflow_parser import parse_workflow
from adaptive_router import route_task
from market_agent import market_analysis
from health_agent import health_analysis
from meeting_agent import create_zoom_meeting
from report_agent import generate_report
from judge_agent import validate_output


def main_workflow(user_input, user_id):
    # For demonstration, use a hardcoded parsed tasks list (in practice, parse_workflow would return JSON)
    tasks = [
        {"task": "Analyze market trends in AI healthcare", "type": "market"},
        {"task": "Assess squad health using wearable data", "type": "health"},
        {"task": "Schedule a review meeting", "type": "meeting"},
        {"task": "Generate a mission report", "type": "report"}
    ]

    results = {}

    for t in tasks:
        task_type = t["type"]
        description = t["task"]

        # Route the task

        service_info = route_task(task_type, description)
        print(f"Routing '{description}' to provider: {service_info['provider']}")

        # Execute based on type
        if task_type == "market":
            result = market_analysis(description)
            results["market"] = result

        elif task_type == "health":
            result = health_analysis(description, user_id)
            results["health"] = result

            # Simulate Judge Agent validating a dummy health metric (e.g., heart_rate from Terra data)
            # Here we use the dummy datum from health_agent.py (heart_rate=72)
            dummy_health_data = {"heart_rate": 72}
            valid, message = validate_output("health", dummy_health_data)
            if not valid:
                results["health_validation"] = message
            else:
                results["health_validation"] = "Health output validated."

        # elif task_type == "meeting":
        #     result = create_zoom_meeting(description)
        #     results["meeting"] = result

        elif task_type == "report":
            result = generate_report(description)
            results["report"] = result

            # Validate report output using Judge Agent
            valid, message = validate_output("report", result)
            if not valid:
                results["report_validation"] = message
            else:
                results["report_validation"] = "Report output validated."

    return results


if __name__ == "__main__":
    # Example usage of the entire workflow
    user_prompt = ("Assess squad readiness using wearable data, schedule a review meeting, "
                   "and generate a mission report.")
    user_id = "demo_user"
    workflow_results = main_workflow(user_prompt, user_id)
    print("Workflow Results:", workflow_results)
