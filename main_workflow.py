#!/usr/bin/env python
"""
main_workflow.py
Integrates all agent modules into a unified workflow.
Parses user workflow, routes tasks, executes each specialized agent,
and validates outputs. Also enriches the final report with the DAIN Agent.
"""

from workflow_parser import parse_workflow
from adaptive_router import route_task
from market_agent import market_analysis
from health_agent import health_analysis
from meeting_agent import create_zoom_meeting
from report_agent import generate_report
from judge_agent import validate_output
from dain_agent import enhance_analysis


def main_workflow(user_input, user_id):
    # For demo, using hardcoded tasks. In production, parse_workflow would generate a JSON list.
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
        service_info = route_task(task_type, description)

<<<<<<< Updated upstream
        # Route the task

        service_info = route_task(task_type, description)
        print(f"Routing '{description}' to provider: {service_info['provider']}")
=======
        # Debugging: print type and content of service_info.
        print("DEBUG: service_info type:", type(service_info))
        print("DEBUG: service_info content:", service_info)
>>>>>>> Stashed changes

        if task_type == "market":
            results["market"] = market_analysis(description)
        elif task_type == "health":
<<<<<<< Updated upstream
            results["health"] = health_analysis(description, user_id)
            # Validate with dummy health data (heart_rate assumed to be 72).
            dummy_health = {"heart_rate": 72}
            valid, message = validate_output("health", dummy_health)
            results["health_validation"] = message
        elif task_type == "meeting":
            results["meeting"] = create_zoom_meeting(description)
=======
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


>>>>>>> Stashed changes
        elif task_type == "report":
            results["report"] = generate_report(description)
            valid, message = validate_output("report", results["report"])
            results["report_validation"] = message

    # Use the DAIN Agent to enhance the final report.
    initial_report = results.get("report", "No report generated.")
    results["enhanced_report"] = enhance_analysis(initial_report, user_id)

    return results


if __name__ == "__main__":
    user_prompt = (
        "Assess squad readiness using wearable data, schedule a review meeting, and generate a mission report."
    )
    user_id = "demo_user_001"
    workflow_results = main_workflow(user_prompt, user_id)
    print("Workflow Results:", workflow_results)
