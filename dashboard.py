#!/usr/bin/env python
"""
dashboard.py
A Streamlit dashboard that provides a real-time view of the complete workflow.
Users can input a workflow, and the system executes the entire process,
displaying results from all agents and validations.
"""

import streamlit as st
from main_workflow import main_workflow

# Set Streamlit page configuration
st.set_page_config(layout="wide")

# Title and description
st.title("MetaAligner SquadOps Dashboard")
st.markdown("This dashboard executes a unified AI workflow for squad-level health command operations.")

# Input fields for workflow and user ID
user_input = st.text_area("Enter your workflow in natural language:",
    "Assess squad readiness using wearable data, schedule a review meeting, and generate a mission report.")
user_id = st.text_input("Enter your User ID:", "demo_user")

# Run the workflow when the button is clicked
if st.button("Run Workflow"):
    st.write("Processing workflow, please wait...")

    results = main_workflow(user_input, user_id)
    st.subheader("Workflow Results")
    st.json(results)
