from openai import OpenAI

client = OpenAI(api_key=os.getenv('OpenAIAPI'))
import os

# Load API key

# Retrieve a list of available models
models = client.models.list()

# Print the model IDs
for model in models.data:
    print(model['id'])