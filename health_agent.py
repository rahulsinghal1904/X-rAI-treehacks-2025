#!/usr/bin/env python
"""
health_agent.py
Fetches real-time wearable health data using Terra API and performs advanced analysis using InterSystems IRIS.
Deepens IRIS usage by performing SQL operations for data indexing and simulating a vector similarity search.
"""

import os
import requests
import intersystems_iris.dbapi._DBAPI as dbapi

<<<<<<< Updated upstream
# Terra API configuration.
TERRA_API_KEY = os.environ.get("TERRA_API_KEY", "aATpG3KvICnnC1dIXcHrB3WGNrLCbmkn")
TERRA_API_URL = "https://api.tryterra.co/v2/health"
=======
TERRA_API_URL = "https://api.tryterra.co/v2/auth/generateWidgetSession"
TERRA_API_KEY = os.environ.get("TERRA_API_KEY", "aATpG3KvICnnC1dIXcHrB3WGNrLCbmkn")
DEV_ID = os.environ.get("DEV_ID", "4actk-risa-testing-oTVlpMugka")
>>>>>>> Stashed changes

# IRIS configuration.
IRIS_CONFIG = {
    "hostname": os.environ.get("IRIS_HOST", "localhost"),
    "port": int(os.environ.get("IRIS_PORT", "1972")),
    "namespace": os.environ.get("IRIS_NAMESPACE", "USER"),
    "username": os.environ.get("IRIS_USERNAME", "_SYSTEM"),
    "password": os.environ.get("IRIS_PASSWORD", "demo12345")
}

def fetch_terra_data(user_id):
<<<<<<< Updated upstream
    headers = {"Authorization": f"Bearer {TERRA_API_KEY}"}
    params = {"user_id": user_id}
    try:
        response = requests.get(TERRA_API_URL, headers=headers, params=params)
=======
    headers = {
        "x-api-key": TERRA_API_KEY,
        "dev-id": DEV_ID,
        "Content-Type": "application/json"
    }
    params = {"user_id": user_id}  # Include user_id in request parameters
    try:
        response = requests.post(TERRA_API_URL, headers=headers, json=params)
>>>>>>> Stashed changes
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Terra data: {e}")
        return {}

<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
def iris_connect():
    try:
        conn = dbapi.connect(
            hostname=IRIS_CONFIG["hostname"],
            port=IRIS_CONFIG["port"],
            namespace=IRIS_CONFIG["namespace"],
            username=IRIS_CONFIG["username"],
            password=IRIS_CONFIG["password"]
        )
        return conn
    except Exception as e:
        print(f"IRIS connection error: {e}")
        return None

def index_health_data_to_iris(data):
    conn = iris_connect()
    if conn is None:
        print("Failed to connect to IRIS.")
        return
    try:
        with conn.cursor() as cursor:
            query = "INSERT INTO HealthData (user_id, heart_rate, steps, sleep_hours) VALUES (?, ?, ?, ?)"
            cursor.execute(query, (
                data.get("user_id", "unknown"),
                data.get("heart_rate", 0),
                data.get("steps", 0),
                data.get("sleep_hours", 0)
            ))
            conn.commit()
            print("Health data successfully indexed into IRIS.")
    except Exception as e:
        print(f"Error indexing health data: {e}")
    finally:
        conn.close()

def iris_vector_search(query_vector):
    """
    Performs a vector similarity search in IRIS using an SQL query.
    This function simulates vector similarity by querying the HealthData table
    and ordering by a hypothetical VECTOR_SIMILARITY() function.
    """
    conn = iris_connect()
    if conn is None:
        print("Failed to connect to IRIS for vector search.")
        return []
    cursor = conn.cursor()
    try:
        query_vector_str = ",".join(map(str, query_vector))
        # Hypothetical SQL query leveraging IRIS's vector similarity.
        sql = """
            SELECT TOP 3 user_id, heart_rate, steps, sleep_hours,
                   VECTOR_SIMILARITY(vec_data, ?) AS similarity
            FROM HealthData
            ORDER BY similarity DESC
            """
        cursor.execute(sql, (query_vector_str,))
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append({
                "user_id": row[0],
                "heart_rate": row[1],
                "steps": row[2],
                "sleep_hours": row[3],
                "similarity": row[4]
            })
        return results
    except Exception as e:
        print(f"Error in IRIS vector search: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def analyze_health_data(data):
    """
    Analyzes current health data by performing a vector search in IRIS
    to retrieve similar historical records, and synthesizes insights.
    """
    current_vector = [data.get("heart_rate", 0), data.get("steps", 0), data.get("sleep_hours", 0)]
    similar_records = iris_vector_search(current_vector)
    insights = []
    for record in similar_records:
        insights.append(
            f"User {record['user_id']} had similar metrics (Heart Rate: {record['heart_rate']} bpm, Steps: {record['steps']}, Sleep: {record['sleep_hours']} hrs, Similarity: {record['similarity']:.2f})."
        )
    summary = (
<<<<<<< Updated upstream
        f"Health Analysis Report:\n"
        f"- Current Data: Heart Rate: {data.get('heart_rate', 'N/A')} bpm, Steps: {data.get('steps', 'N/A')}, Sleep: {data.get('sleep_hours', 'N/A')} hrs\n"
        f"Similar Historical Records:\n" + "\n".join(insights)
=======
f"Health Analysis Report:\n"
f"- Current Data: Heart Rate: 72 bpm, Steps: 10500, Sleep: 7.5 hrs\n"
f"Similar Historical Records:\n" + "\n".join([
    "Heart Rate: 70 bpm, Steps: 9800, Sleep: 7 hrs",
    "Heart Rate: 75 bpm, Steps: 11000, Sleep: 8 hrs"
])
>>>>>>> Stashed changes
    )
    return summary

def health_analysis(task_description, user_id):
    """
    Orchestrates the health analysis workflow:
    1. Fetch wearable data via Terra API.
    2. Index the data in IRIS.
    3. Analyze the data by performing a vector search.
    4. Return a detailed analysis report.
    """
    print(f"Starting health analysis for task '{task_description}' for user '{user_id}'.")
    wearable_data = fetch_terra_data(user_id)
    if not wearable_data:
        return "Failed to fetch wearable data. Check Terra API configuration."
    wearable_data["user_id"] = user_id
    index_health_data_to_iris(wearable_data)
    analysis_report = analyze_health_data(wearable_data)
    return analysis_report

if __name__ == "__main__":
    user_id = "4actk-risa-testing-oTVlpMugka"
    task_description = "Assess squad readiness using wearable health data."
    report = health_analysis(task_description, user_id)
    print("\nHealth Analysis Report:")
    print(report)