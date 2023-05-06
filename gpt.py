import os

import streamlit as st
import openai


class OpenAIService:
    def __init__(self):
        openai.api_key = os.environ["OPENAI_API_KEY"]

    def list_models(self):
        return openai.Model.list()

    def prompt(self, prompt):
        return openai.ChatCompletion.create(
            # model="gpt-3.5-turbo",
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            timeout=60,
        )
