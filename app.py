import os
import io

import pandas as pd
import streamlit as st
import snowflake.connector

from gpt import OpenAIService


st.set_page_config(layout="wide")


with st.sidebar:
    st.caption("Snowflake Credentials")
    os.environ["SNOWFLAKE_USER"] = st.text_input("User", value="aziz")
    os.environ["SNOWFLAKE_PASSWORD"] = st.text_input("Password", value="foobar", type="password")
    os.environ["SNOWFLAKE_ACCOUNT"] = st.text_input("Account", value="hvb31278.us-east-1")
    os.environ["SNOWFLAKE_WAREHOUSE"] = st.text_input("Warehouse", value="COMPUTE_WH")
    os.environ["SNOWFLAKE_SCHEMA"] = st.text_input("Schema", value="TPCH_SF1")
    os.environ["SNOWFLAKE_DATABASE"] = st.text_input("Database", value="SNOWKATHAON")

    st.write("---")
    api_key = st.text_input("OpenAI API Key", type="password")
    if api_key:
        os.environ["OPENAI_API_KEY"] = api_key
    else:
        st.info("Please enter OpenAI API Key")
        st.stop()


creds = {
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
}


@st.cache_resource
class SnowflakeDB:
    def __init__(self) -> None:
        self.conn = snowflake.connector.connect(**creds)
        self.cursor = self.conn.cursor()


def read_prompt_file(fname):
    with open(fname, "r") as f:
        return f.read()


@st.cache_data
def query(_conn, query):
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.warning("Error in query")
        st.error(e)


@st.cache_data
def ask(prompt):
    response = gpt.prompt(prompt)
    return response["choices"][0]["message"]["content"]


def get_tables_schema(_conn):
    with st.expander("View tables schema in database"):
        table_schemas = """"""
        df = query(_conn, "show tables")
        st.write(df)
        for table in df["name"]:
            t = f"{DATABASE_NAME}.{SCHEMA_NAME}.{table}"
            df = query(sf.conn, f"select * from {t} limit 10;")
            ddl_query = f"select get_ddl('table', '{t}');"
            ddl = query(sf.conn, ddl_query)
            schema = f"\n{ddl.iloc[0,0]}\n"
            st.write(f"### {table}")
            st.markdown(f"```sql{schema}```")
            st.write("---")

            table_schemas = table_schemas + f"\n{table}\n"
            table_schemas = table_schemas + f"{schema}\n"

    return table_schemas


def get_sample_questions(table_schemas):
    with st.expander("Sample Questions"):
        prompt = read_prompt_file("sample_questions_prompt.txt")
        prompt = prompt.replace("<<TABLES>>", table_schemas)
        answer = ask(prompt)
        st.code(answer)


def df_schema(df):
    # -- data schema

    sio = io.StringIO()
    df.info(buf=sio)
    df_info = sio.getvalue()
    return df_info


if __name__ == "__main__":
    st.title("SnowChat")
    msg = "Connect to your Snowflake database and ask questions about your data and get answers in real-time with visualization supported. Powered by `Streamlit` and `GPT-4`."
    st.write(msg)

    gpt = OpenAIService()
    sf = SnowflakeDB()

    SCHEMA_NAME = os.environ["SNOWFLAKE_SCHEMA"]
    DATABASE_NAME = os.environ["SNOWFLAKE_DATABASE"]
    WAREHOUSE_NAME = os.environ["SNOWFLAKE_WAREHOUSE"]

    # -- get tables DDL in schema
    table_schemas = get_tables_schema(sf.conn)

    # -- sample questions
    sample_questions = get_sample_questions(table_schemas)

    st.write("---")

    # -- ask SQL question
    question = st.text_area(
        "Ask a question about the database data",
        placeholder="What is the total revenue?",
    )
    if question:
        # -- curate prompt
        prompt = read_prompt_file("sql_prompt.txt")
        prompt = prompt.replace("<<TABLES>>", table_schemas)
        prompt = prompt.replace("<<QUESTION>>", question)
        answer = ask(prompt)
        st.code(answer)
    else:
        st.stop()

    # -- parse response
    # if st.checkbox("Run", key="answer") and answer:
    df = query(sf.conn, answer)
    st.dataframe(df, use_container_width=True)

    # -- ask Python question
    question = st.text_input(
        "Ask a question about the result",
        placeholder="e.g. Visualize the data",
    )
    if question:
        # -- curate prompt
        df_info = df_schema(df)

        prompt = read_prompt_file("python_prompt.txt")
        prompt = prompt.replace("<<DATAFRAME>>", df_info)
        prompt = prompt.replace("<<QUESTION>>", question)
        answer = ask(prompt)
        with st.expander("view generated code"):
            st.code(answer)
        clean = answer.replace("```python", "") # hotfix
        clean = answer.replace("```", "")
        exec(clean)
