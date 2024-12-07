import os
import json
from datetime import datetime
from utils.log_utils import *
from utils.string_utils import create_hash_sha256, clean_markdown_json
from openai import OpenAI


class OpenAiClient:
    """
    Singleton class for OpenAI ChatGPT text sentiment detection
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance  # Always return the instance

    def __init__(self, openai_api_key: str = ""):
        # Check if the instance already exists
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.openai_model = "gpt-3.5-turbo"
            self.openai_client = OpenAI(api_key=openai_api_key)

    def query(self,
              prompt: str,
              role: str = "You are a financial analyst",
              cache_data: bool = False,
              cache_dir: str = "cache"):
        try:
            # Generate a unique cache file name based on symbol, prompt, and current date
            hash_key = create_hash_sha256(f"{prompt}")
            today_str = datetime.today().strftime('%Y-%m-%d')
            file_name = f"openai_response_{hash_key}_{today_str}.json"
            cache_path = os.path.join(cache_dir, file_name)

            # Check if cached data exists for today
            if cache_data and os.path.exists(cache_path):
                logi(f"Loading cached response for from {cache_path}")
                with open(cache_path, "r") as cache_file:
                    cached_data = json.load(cache_file)
                return cached_data

            # If no cache exists, send request to OpenAI
            messages = [
                {"role": "system", "content": role},
                {"role": "user", "content": prompt}
            ]

            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                temperature=0,
                messages=messages
            )
            print(response)

            # Extract the response content
            response_content = response.choices[0].message.content.strip()

            # Clean Markdown code block tags
            cleaned_content = clean_markdown_json(response_content)

            # Cache the response if valid
            if cache_data and cleaned_content is not None:
                os.makedirs(cache_dir, exist_ok=True)  # Ensure cache directory exists
                with open(cache_path, "w") as cache_file:
                    cache_file.write(cleaned_content)
                logi(f"Cached response for {cache_path}")

            return cleaned_content

        except Exception as ex:
            loge(f"Exception in OpenAiClient: {ex}")
            return None
