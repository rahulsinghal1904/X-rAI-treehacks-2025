#!/usr/bin/env python
"""
health_agent.py
Integrates Terra API to fetch real-time wearable health data for squad members.
Uses Elasticsearch for RAG-based analysis to identify trends in heart health and readiness.
"""

import requests
from elasticsearch import Elasticsearch

# Terra API configuration
TERRA_API_KEY = "YOUR_TERRA_API_KEY"  # Replace with your actual Terra API key
TERRA_API_URL = "https://api.tryterra.co/v2/health"

# Elasticsearch configuration
ES_HOST = "http://localhost:9200"
ES_INDEX = "health-data"


def fetch_terra_data(user_id):
    """
    Fetch real-time wearable health data for a squad member using Terra API.
    Args:
        user_id (str): The unique ID of the user whose data is being fetched.
    Returns:
        dict: A dictionary containing wearable health data (e.g., heart rate, steps, sleep).
    """
    headers = {"Authorization": f"Bearer {TERRA_API_KEY}"}
    params = {"user_id": user_id}

    try:
        response = requests.get(TERRA_API_URL, headers=headers, params=params)
        response.raise_for_status()  # Raise an error for bad HTTP responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Terra data: {e}")
        return {}


def index_health_data_to_elasticsearch(data):
    """
    Index the fetched health data into Elasticsearch for further analysis.
    Args:
        data (dict): The health data to be indexed.
    Returns:
        None
    """
    es = Elasticsearch([ES_HOST])

    try:
        # Index the document into Elasticsearch
        es.index(index=ES_INDEX, body=data)
        print("Health data successfully indexed into Elasticsearch.")
    except Exception as e:
        print(f"Error indexing data to Elasticsearch: {e}")


def analyze_health_data(data):
    """
    Analyze the wearable health data using RAG techniques with Elasticsearch.
    Args:
        data (dict): The wearable health data to be analyzed.
    Returns:
        str: A summary of the analysis.
    """
    es = Elasticsearch([ES_HOST])

    try:
        # Query Elasticsearch for similar historical records
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"heart_rate": data.get("heart_rate", 0)}},
                        {"range": {"steps": {"gte": 5000}}}  # Example threshold for steps
                    ]
                }
            }
        }

        response = es.search(index=ES_INDEX, body=query)

        # Extract insights from the retrieved documents
        insights = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            insights.append(f"User {source['user_id']} had a similar heart rate of {source['heart_rate']} bpm.")

        # Generate a summary based on the analysis
        summary = (
                f"Health Analysis:\n"
                f"- Heart Rate: {data.get('heart_rate', 'N/A')} bpm\n"
                f"- Steps: {data.get('steps', 'N/A')}\n"
                f"- Sleep Hours: {data.get('sleep_hours', 'N/A')}\n"
                f"\nInsights:\n" + "\n".join(insights)
        )

        return summary

    except Exception as e:
        print(f"Error analyzing health data: {e}")
        return "Error analyzing health data."


def health_analysis(task_description, user_id):
    """
    Main function to perform health analysis by fetching and analyzing wearable data.
    Args:
        task_description (str): A description of the task being performed.
        user_id (str): The unique ID of the user whose health is being analyzed.
    Returns:
        str: A detailed report of the health analysis.
    """
    print(f"Starting health analysis for task: '{task_description}' and user ID: {user_id}")

    # Step 1: Fetch wearable health data from Terra API
    wearable_data = fetch_terra_data(user_id)

    if not wearable_data:
        return "Failed to fetch wearable data. Please check the Terra API configuration."

    # Step 2: Index the fetched data into Elasticsearch for RAG-based analysis
    index_health_data_to_elasticsearch(wearable_data)

    # Step 3: Analyze the indexed data and generate insights
    analysis_report = analyze_health_data(wearable_data)

    return analysis_report


if __name__ == "__main__":
    # Example usage of health_analysis function
    user_id = "demo_user_001"

    task_description = "Assess squad readiness using wearable health data."

    report = health_analysis(task_description, user_id)

    print("\nHealth Analysis Report:")
    print(report)
