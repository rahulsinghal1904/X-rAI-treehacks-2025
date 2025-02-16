#!/usr/bin/env python
"""
workflow_parser.py
Parses a natural language workflow into actionable JSON subtasks using LangChain and GPT-4.
"""

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI

def parse_workflow(user_input):
    # Define a prompt template to decompose the workflow into JSON subtasks.
    prompt = PromptTemplate(
        input_variables=["workflow"],
        template=("Decompose the following workflow into JSON subtasks with 'task' and 'type' fields. "
                  "Valid types are 'health', 'market', 'meeting', or 'report'.\n\n{workflow}\n")
    )
    chain = LLMChain(llm=OpenAI(model="gpt-4"), prompt=prompt)
    response = chain.run(workflow=user_input)
    return response

if __name__ == "__main__":
    user_input = ("Assess squad readiness using wearable data, schedule a review meeting, and generate a mission report.")
    parsed = parse_workflow(user_input)
    print("Parsed Workflow:", parsed)
