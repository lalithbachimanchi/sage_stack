import openai

# Replace 'YOUR_API_KEY' with your actual OpenAI API key
api_key = 'YOUR_API_KEY'

# Initialize the OpenAI API client
openai.api_key = api_key

# Function to interact with ChatGPT
def chat_with_gpt(prompt):
    response = openai.Completion.create(
        engine="davinci",  # You can choose a different engine like "text-davinci-002" if needed.
        prompt=prompt,
        max_tokens=50  # Adjust this value based on the desired response length.
    )
    return response.choices[0].text.strip()

# Main loop for user interaction
while True:
    user_input = input("You: ")
    if user_input.lower() == "exit":
        print("ChatGPT: Goodbye!")
        break
    prompt = f"You: {user_input}\nChatGPT:"
    response = chat_with_gpt(prompt)
    print("ChatGPT:", response)
