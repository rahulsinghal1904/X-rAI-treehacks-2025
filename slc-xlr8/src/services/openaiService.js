// src/services/openaiService.js
import axios from "axios";
import RFD_PROMPT from "../prompts/rfp_analyzer";
import DATA_PROMPT from "../prompts/data_analyzer";
import EPICS_PROMPT from "../prompts/epics";
import SCREEN_MOCKUP_PROMPT from "../prompts/screen_mockup";
import TECHNICAL_DESIGN_PROMPT from "../prompts/technical_design";

export const getOpenAIResponse = async (userInput, prompt) => {
  const openAIApiKey = import.meta.env.VITE_OPENAI_API_KEY;
  if (!openAIApiKey) {
    throw new Error(
      "OpenAI API key is not defined in the environment variables."
    );
  }
  const apiUrl = "https://api.openai.com/v1/chat/completions";

  let systemPrompt = "";

  switch (prompt) {
    case "rfp_analyzer":
      systemPrompt = RFD_PROMPT;
      break;
    case "data_analyzer":
      systemPrompt = DATA_PROMPT;
      break;
    case "epics":
      systemPrompt = EPICS_PROMPT;
      break;
    case "screen_mockup":
      systemPrompt = SCREEN_MOCKUP_PROMPT;
      break;
    case "technical_design":
      systemPrompt = TECHNICAL_DESIGN_PROMPT;
      break;
    default:
      systemPrompt = "";
  }
  console.log(systemPrompt);

  const payload = {
    model: "gpt-4", // Make sure you have access to GPT-4
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userInput },
    ],
    temperature: 0.7, // Adjust as needed
  };

  try {
    const response = await axios.post(apiUrl, payload, {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openAIApiKey}`,
      },
    });

    console.log(response.data.choices[0]);

    return response.data.choices[0].message.content;
  } catch (error) {
    console.error("Error in getOpenAIResponse:", error);
    if (error.response) {
      const errorDetails = error.response.data;
      throw new Error(
        `OpenAI API error: ${error.response.status} ${error.response.statusText} - ${errorDetails.error.message}`
      );
    } else {
      throw error;
    }
  }
};
