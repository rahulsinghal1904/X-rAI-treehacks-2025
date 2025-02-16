from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI

def parse_workflow(user_input):
    prompt = PromptTemplate(
        input_variables=["workflow"],
        template=("Decompose the following workflow into JSON subtasks with 'task' "
                  "and 'type' fields (e.g., health, meeting, report):\n\n{workflow}")
    )
    chain = LLMChain(llm=OpenAI(model="gpt-4"), prompt=prompt)
    response = chain.run(workflow=user_input)
    return response

# Example usage
if __name__ == "__main__":
    user_input = ("Assess squad readiness using wearable data, "
                  "schedule a review meeting, "
                  "and generate a mission report.")
    print(parse_workflow(user_input))
