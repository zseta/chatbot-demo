from groq import Groq
from config import GROQ_API_KEY

class LLMProvider:    
    def __init__(self, model_name="llama-3.1-8b-instant"):
        self.model_name = model_name
        self.client = Groq(api_key=GROQ_API_KEY)
        
    def generate_response_stream(self, system_prompt: str, prompt: str):
        messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ]
        response = self.client.chat.completions.create(
                            model=self.model_name,
                            messages=messages, # type: ignore
                            stream=True,
                            max_tokens=150
                        )
        for chunk in response:
            content = chunk.choices[0].delta.content
            if content:
                yield content
        