from openai import OpenAI
import os

# Initialize the OpenAI client
client = OpenAI(api_key=os.getenv('OpenAIAPI'))

# List available models
models = client.models.list()

# Iterate through the list of models
for model in models:
    print(model.id)