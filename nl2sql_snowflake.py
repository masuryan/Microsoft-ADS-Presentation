import os
from openai import AzureOpenAI 
import json

import snowflake.connector

import pandas as pd
import re

csv_query_data = ""

class snowflakeMetaData:
    table_info = {
    "PURCHASE_HISTORY": {
        "columns": ["ID", "MED_AGE", "RECENT_PURCHASES", "ZIP"],
        "primary_key": "ID"
        }
    }   
    
    business_terminology = [
        { "table column": "ID",
            "business_terms": ["Identity", "Unique Number"],
           "values": ["001", "002", "003", "004"],
         "description": "This column has running identity number which is unique and incremental by 1"
        },
        { "table column": "MED_AGE",
           "business_terms": ["Age", "Customer Age", "Purchaser Age"],
           "values" : ["29", "59", "60"],
         "description": "This column has information about age of the human who is purchaser, Customer, Client"
        },
        {  "table column": "ZIP",
         "business_terms": ["Zip Code", "US Zip code", "Zip number", "Zip", "Locations"],
           "values": ["US Zip codes"],
         "description": "This column has purchaser zip codes of USA"
        },
        {  "table column": "RECENT_PURCHASES",
        "values": ["1 Gallon Milk, 24 oz Bread, Dozen Eggs", "5 lb Potatoes, 3 lb Onions, 1 lb Carrots" , "Bunch of Bananas, 1 lb Grapes, 16 oz Strawberries", "1.5 qt Ice Cream, 12 inch Frozen Pizza, 16 oz Frozen Vegetables"],
         "description":"This column has information about purchases done by the customer on retail store"
        }
    ]

    dateformat = {
    "DateId": "20140115",
    "MonthYear" : "Jan-24",
    "QuarterYear" : "Q3-2024"
    }

    todaydate = "20240327"
    yearstartdate = "20240101"
    yearenddate = "20241231"

class SystemPrompts:
    promptToGenerateSnowflakeQuery = f"""
    Given an input question, use Snowflake datawarehouse SQL syntax to generate an SQL query by choosing one or multiple tables.
    Write the query in between <SQL></SQL>.

    you can use the following table schema of snowflake database:
    <table_schema>{snowflakeMetaData.table_info}</table_schema>

    Date format used in the warehouse is:
    {snowflakeMetaData.dateformat}todays date is: {snowflakeMetaData.todaydate}

    Here is the business terminology for columns:
    {snowflakeMetaData.business_terminology}

    Other instructions
    1. Make sure you don't give SQL syntax errors and you always give best performing query as you have the Indexing information available in the Table schema

    """

    promptToGenerateAnswerFromPandas = f"""
    Given an input question, you need to remove the filter filters, joins and summaries in the question. use the following CSV data to answer. This {csv_query_data} was generated after executing a SQL query on Snowflake database and this data has data needed to answer the question, so reply back in crisp and clear english lanuage and don't include any notes or additional comments

    you can use the following table schema of snowflake database:
    <table_schema>{snowflakeMetaData.table_info}</table_schema>

    Date format used in the warehouse is:
    {snowflakeMetaData.dateformat}todays date is: {snowflakeMetaData.todaydate}

    Here is the business terminology for columns:
    {snowflakeMetaData.business_terminology}

    Other instructions
    1. Make sure you only refer data inside the CSV and you will not search outside of this
    2. You don't have to summarize or join or filter or any other activities as it is already pre-calculated, you just answer it in simple english

    Example:
    1. If the input questions is "Tell me purchases done by people who age is 50" you only consider "Telling about Purchases" and ignore the age factor

    """

class OpenAICalls():
    AZURE_OPENAI_ENDPOINT="https://XXX.openai.azure.com/"
    AZURE_OPENAI_API_KEY="XXX"
    api_version = "2024-02-15-preview"

    client = AzureOpenAI(
        azure_endpoint = AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,  
        api_version=api_version
    )
    deployment_name='mjvisiongpt' #This will correspond to the custom name you chose for your deployment when you deployed a model. Use a gpt-35-turbo-instruct deployment. 
    def RunMyQquery(systesmprompt, usersprompt):
    # Step 1: send the conversation and available functions to the model
        messages = [{"role": "system", "content": systesmprompt},
                {"role": "user", "content": usersprompt}]
    
        #print("********* part1 ***********,\n", messages)
        response = OpenAICalls.client.chat.completions.create(
            model=OpenAICalls.deployment_name,
            messages=messages,
            seed = 2024,
            temperature = 0.2
        )
        return response.choices[0].message.content
    

class SnowflakeHelper:
    # Set your Snowflake credentials
    snowflake_config = {
        'user': 'XXX',
        'password': 'XXX',
        'account': 'ljctvqh-ej14293',
        'warehouse': 'COMPUTE_WH',
        'database': 'RETAIL_DB',
        'schema': 'PUBLIC'
    }
    conn = snowflake.connector.connect(
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        account=snowflake_config['account'],
        warehouse=snowflake_config['warehouse'],
        database=snowflake_config['database'],
        schema=snowflake_config['schema']
    )

    def query2csv(sql_query):
        # Execute the query and fetch the results
        cursor = SnowflakeHelper.conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # Convert the results to a pandas DataFrame
        df = pd.DataFrame(results, columns=[col[0] for col in cursor.description])

        # Close the connection, convert Dataframe to CSV and return it as string having Data in CSV format
        cursor.close()
        SnowflakeHelper.conn.close()
        csv_query_data = df.to_csv(index=False)
        return csv_query_data


user_q1 = "Show me the top 5 recent purchases done by customer having age above 50"
user_q2 = "Show me the top 5 locations where most of the purchases are made with number of purchases"
user_q3 = "what are the purchase related to dairy products done by custers who are 50 years or more old"
user_q4 = "what are the total number of purchases done till now"
user_q5 = "how many people brought eggs?"

user_q = user_q1

# STEP 1 : Ask OpenAI to generate the SQL query

question = f"""
Please provide the SQL query for this question:
Question: {user_q}
Query: <SQL></SQL>
"""
sqlFromSnowflake = OpenAICalls.RunMyQquery(SystemPrompts.promptToGenerateSnowflakeQuery,question)

sql_query = re.search(r"<SQL>(.*?)</SQL>", sqlFromSnowflake, re.DOTALL).group(1)

# STEP 2 : Execute the SQL Query on Snowflake Database and return the CSV as string


csv_query_data = SnowflakeHelper.query2csv(sql_query)
print(sql_query)

# STEP 3: Ask the same question by giving the CSV data (RAG Pattern) and answer in english language and tell the model not to search outside the CSV

user_qs = "Answer" + user_q + "by using the following CSV data" + csv_query_data

response = OpenAICalls.RunMyQquery(SystemPrompts.promptToGenerateAnswerFromPandas, user_qs)

print(response)