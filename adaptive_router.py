#!/usr/bin/env python
"""
adaptive_router.py
Implements an improved adaptive load balancing strategy that considers estimated token usage,
task type, latency, and cost efficiency, with added debugging to ensure a proper dictionary is returned.
"""

import random

# Define available models/APIs and their metrics.
MODELS = {
    "openai": {"cost_per_token": 0.06, "latency": 200, "max_tokens": 4000},
    "anthropic": {"cost_per_token": 0.04, "latency": 300, "max_tokens": 8000},
    "mistral": {"cost_per_token": 0.03, "latency": 500, "max_tokens": 2000}
}

# Initial simulated performance averages.
performance_metrics = {
    "openai": {"avg_latency": 200, "avg_cost": 0.06},
    "anthropic": {"avg_latency": 300, "avg_cost": 0.04},
    "mistral": {"avg_latency": 500, "avg_cost": 0.03}
}


def estimate_token_usage(task_description):
    """
    Estimate token usage based on word count (~1.3 tokens per word).
    """
    words = len(task_description.split())
    return int(words * 1.3)


def route_task(task_type, task_description):
    """
    Routes task according to estimated token usage, cost, and latency.
    Returns a dictionary with model metrics.
    """
    estimated_tokens = estimate_token_usage(task_description)
    eligible_models = {model: metrics for model, metrics in MODELS.items() if metrics["max_tokens"] >= estimated_tokens}

    # Debug: Print eligible models.
    # print("Eligible models:", eligible_models)

    scores = {}
    for model, metrics in eligible_models.items():
        cost_score = performance_metrics[model]["avg_cost"] / metrics["cost_per_token"]
        latency_score = performance_metrics[model]["avg_latency"] / metrics["latency"]
        scores[model] = cost_score + latency_score

    if scores:
        selected_model = max(scores, key=scores.get)
    else:
        # Fallback to default dictionary if no eligible model exists.
        selected_model = "openai"

    update_feedback(selected_model)

    result = eligible_models.get(selected_model, {"provider": selected_model, "cost": 0.06, "latency": 200})

<<<<<<< Updated upstream

    return {"provider": selected_model}

=======
    # In case result is not a dictionary, force it into one.
    if isinstance(result, str):
        result = {"provider": result, "cost": MODELS[result]["cost_per_token"], "latency": MODELS[result]["latency"]}

    return result
>>>>>>> Stashed changes


def update_feedback(model_name):
    """
    Simulates updating performance metrics with a new measured latency and cost.
    """
    new_latency = random.randint(150, 500)
    new_cost = MODELS[model_name]["cost_per_token"] * random.uniform(0.9, 1.1)
    performance_metrics[model_name]["avg_latency"] = (
            0.8 * performance_metrics[model_name]["avg_latency"] + 0.2 * new_latency
    )
    performance_metrics[model_name]["avg_cost"] = (
            0.8 * performance_metrics[model_name]["avg_cost"] + 0.2 * new_cost
    )


if __name__ == "__main__":
    task_type = "health"
    task_description = "Analyze squad health using wearable data."
    service_info = route_task(task_type, task_description)
    print("Service Info (type):", type(service_info))
    print("Routing '", task_description, "' to provider:", service_info["provider"])
