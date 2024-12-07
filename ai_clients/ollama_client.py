import ollama
from utils.log_utils import *


class OllamaClient:
    def __init__(self):
        self.model = 'llama3.2'

    def query(self, prompt: str, role: str = 'Financial analyst'):
        try:
            # Ensure input is properly formatted as a dictionary
            messages = [
                {'role': 'system', 'content': role},
                {'role': 'user', 'content': prompt}
            ]

            # Send the prompt to the model and get the response
            response = ollama.chat(
                model=self.model,
                messages=messages
            )

            # Debugging raw response
            #logd(f"Raw Response: {response}")

            # Optional: Extract and return the specific response content
            content = response.get('message', {}).get('content', '')
            #logd(f"Extracted Content: {content}")

            return content
        except Exception as ex:
            loge(f"Exception in OllamaClient: {str(ex)}")
            return None


