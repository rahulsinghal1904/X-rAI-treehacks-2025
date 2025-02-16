#!/usr/bin/env python
"""
dashboard.py
A Streamlit dashboard that provides a real-time view of the entire workflow,
including live IRIS metrics and results from multi-agent workflow execution.
"""

import streamlit as st
from main_workflow import main_workflow
from intersystems_iris.dbapi._DBAPI import connect as iris_connect

IRIS_CONFIG = {
    "hostname": "localhost",
    "port": 1972,
    "namespace": "USER",
    "username": "_SYSTEM",
    "password": "demo12345"
}

def ensure_executionlog_table(config):
    try:
        with iris_connect(**config) as conn:
            with conn.cursor() as cursor:
                create_query = (
                    "CREATE TABLE ExecutionLog ("
                    "id INT IDENTITY PRIMARY KEY, "
                    "latency INT"
                    ")"
                )
                insert_query = (
                    "INSERT INTO ExecutionLog (latency)"
                    "VALUES(250);"
                ")"
                )
                cursor.execute(create_query)
                cursor.execute(insert_query)
                conn.commit()
                st.info("ExecutionLog table created.")
    except Exception as e:
        if "already exists" in str(e) or "Duplicate" in str(e):
            # st.info("ExecutionLog table already exists.")
            pass
        else:
            st.error(f"Error creating ExecutionLog table: {e}")

def fetch_live_metrics():
    ensure_executionlog_table(IRIS_CONFIG)
    avg_latency = None
    try:
        with iris_connect(**IRIS_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT AVG(latency) FROM ExecutionLog")
                result = cursor.fetchone()
                if result is not None:
                    avg_latency = result[0]
    except Exception as e:
        st.error(f"Error fetching metrics from IRIS: {e}")
        avg_latency = None
    return avg_latency

st.set_page_config(layout="wide")
st.title("MetaAligner SquadOps Dashboard")
st.markdown("Unified System for Squad-Level Health Command Operations with Judge & DAIN Agent Integration")

st.subheader("Live IRIS Metrics")
latency = fetch_live_metrics()
if latency is None:
    st.write("No metrics available. Ensure the 'ExecutionLog' table exists and is populated.")
else:
    st.write(f"Current average latency: {latency} ms")

user_input = st.text_area("Enter your workflow:", "Assess squad readiness using wearable data, schedule a review meeting, and generate a mission report.")
user_id = st.text_input("Enter your User ID:", "demo_user_001")

if st.button("Run Workflow"):
    st.write("Processing workflow, please wait...")
    results = main_workflow(user_input, user_id)
    st.subheader("Workflow Results")
    st.json(results)
