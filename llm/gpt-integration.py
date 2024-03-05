import streamlit as st
from openai import OpenAI
import os
import datetime

max_tokens = int(os.environ.get('OPENAI_MAX_TOKENS', '4000'))
model = os.environ.get('OPENAI_MODEL', 'gpt-3.5-turbo')


@st.cache_data
def generate_gpt_response(api_key, prompt):

    messages = [{"role": "system",
                 "content": " Respond with complete details"}]
    if prompt:
        messages.append({"role": "user", "content": f"{prompt}"})

    print(messages)
    try:
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

        response = client.chat.completions.create(
            model=model,
            messages= messages,
            max_tokens=int(max_tokens),
        )
        print(f"Tokens Used for Generating Code: {response.usage.completion_tokens}")
        st.write(f"Tokens Used for generating response: {response.usage.completion_tokens}")

        print(f"GPT Response: {response.json()}")

        return response.choices[0].message.content
    except Exception as e:
        print(f'Exception Occurred On GPT API: {e}')
        return None


def main():
    st.title("Chatbot for LLM")

    with st.sidebar:
        with st.form(key="user_choices", clear_on_submit=False):
            llm_model = st.radio(label='Select LLM Model', options=['Chat GPT', 'Gemini', 'LLAMA2'])
            open_api_key = st.text_input("Enter LLM Auth Key", type="password")
            parse_apis_submitted = st.form_submit_button("Validate Auth Key")

        if parse_apis_submitted:
            if not open_api_key:
                st.error('Please enter your OpenAI API key!', icon='⚠')
                st.stop()
            if open_api_key:
                os.environ["OPENAI_API_KEY"] = open_api_key

    with st.form(key="select_apis", clear_on_submit=True):
        user_input = st.text_area("User Input")

        api_submitted = st.form_submit_button("Generate LLM Response")

    if api_submitted:
        resp = generate_gpt_response(api_key=os.environ['OPENAI_API_KEY'],prompt=user_input)
        st.markdown(resp)
        st.download_button(
            label="Download Response",
            data=resp,
            file_name=f"llm_response_{datetime.datetime.now()}.txt",
            mime="text/plain",
            key=f"download_button",
        )

# Run the Streamlit app
if __name__ == "__main__":
    main()
