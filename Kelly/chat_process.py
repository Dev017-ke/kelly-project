#Libraries
#import gradio as gr

import logging
import os
from openai import AzureOpenAI, AsyncAzureOpenAI
from time import sleep
import re
import asyncio, httpx
import async_timeout
#from loguru import logger
#from typing import Optional, List
#from pydantic import BaseModel
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pyodbc
# from pydub import ChatSegment
# from pydub.utils import make_chunks
import wave
import math
import os
import pandas as pd 
import pytz
from openai import AzureOpenAI
from retrying import retry
from time import sleep
from datetime import datetime
import os
import sys
import pandas as pd
import tiktoken
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
encoding = tiktoken.get_encoding("cl100k_base")
input_token_count = 0
output_token_count = 0
Driver='{ODBC Driver 17 for SQL Server}'
Server='atubsu-hranalyticsserver.database.windows.net'
Database='hranalyticsdb'
Uid='HRadmin'
Pwd='Welcome#2021'
account_url = "https://kellygenaiblob.blob.core.windows.net"
#default_credential = DefaultAzureCredential()
default_credential = "iHw9/Wcid+aMCGXCcWkQq6yqhhjUzcrLn0YonYiFkKzR86wZYfw40GGO6X833xT/xe2GQJtX8HzB+AStRG9++g=="
#import librosa
#from retrying import retry
import ast
import json
import logging
import fitz
import io
from PIL import Image
import base64
client = AzureOpenAI(azure_endpoint="https://digitalesgframework.openai.azure.com/",
api_version="2023-07-01-preview",
api_key="3ec1f5fed9ac47dc9e3f643df650a758")

def dataframe_to_string(file_data):
    pdf_document = fitz.open("pdf", file_data)
    # Dictionaries to hold text and images
    text_dict = {}
    images_dict = {}
    pdf_dict = {}
    text_string = ""
    # Iterate over each page
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        # Extract text from the page
        text = page.get_text("text")
        text_dict[page_num + 1] = text
        text_string = text_string + text
        # Extract images from the page
        image_list = page.get_images(full=True)
        images = []
        #Extract PDF as an Image
        pix = page.get_pixmap().tobytes()
        pdf_dict[page_num + 1] = pix
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            # Convert image to base64
            # image_base64 = base64.b64encode(image_bytes).decode('ascii')
            # images.append(image_base64)
            images.append(image_bytes) #Appending Bytes instead of b64 decode
        images_dict[page_num + 1] = images
    #return text_dict, images_dict, pdf_dict
    return text_string
    # string_convo = ""
    # for rows, columns in dataframe.iterrows():
    #     string_convo = string_convo + f'{columns[1]}({columns[2]}) : {columns[3]}\n'
    # return string_convo

def fromListToScore_NA(list1):
    temp_question_output_list = []
    df_chat_question_without_final_weight = pd.read_csv('./Templates/chat_question_lookup_v1.csv')
    df_chat_question_without_final_weight = df_chat_question_without_final_weight.drop('final_weightage', axis = 1)
    list1 = list(map(str.lower, list1))
    df_chat_question_without_final_weight['Answer'] = list1
    df_without_na = df_chat_question_without_final_weight[df_chat_question_without_final_weight['Answer'].isin(['yes','no'])]
    sum_of_provided_weightage = df_without_na['provided_weightage'].sum()
    df_chat_question_without_final_weight['final_weightage'] = df_chat_question_without_final_weight['provided_weightage'] * (100 / sum_of_provided_weightage)
    group_ids_to_update = df_chat_question_without_final_weight.loc[(df_chat_question_without_final_weight['group_fatality_Bool'] == 1) & (df_chat_question_without_final_weight['Answer'] == 'no'), 'group_ID']
    df_chat_question_without_final_weight.loc[df_chat_question_without_final_weight['group_ID'].isin(group_ids_to_update), 'final_weightage'] = 0 
    df_chat_question_without_final_weight.loc[df_chat_question_without_final_weight['Answer'].isin(['no','not applicable']), 'final_weightage'] = 'NA'
    #print(df_chat_question_without_final_weight['final_weightage'].sum())
    return df_chat_question_without_final_weight['final_weightage'].tolist()

def chat_List_First_Score_AI(Transcribed_text,question_list):
    global input_token_count
    global output_token_count
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed audio. The answers for each query will be a 'Yes' or 'No'"
            user_prompt = f"""
                        You will be provided with a conversation, which includes  Customer, Associate ,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed audio. The answers for each query is supposed to provide a score (NUMERIC) for a particular category betwen 0 - 10, where 10 being the best 0 being the worst.
                        Task to perform:
                        --Provide ONLY the python list consisting of ONLY the numeric value for the score.
                        The question list is: ```{question_list}```
                        The transcribed text is:   ```{Transcribed_text}```
                        """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            
            
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                messages = [{"role":"system","content":system_prompt},
                        {"role":"user",
                        "content": user_prompt
                        }
                        ],
                temperature=0,
                max_tokens=800,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                stop=None
                )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list) 
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list)
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                try:
                    print('Trying Json Method to get answer')
                    json_String = test_list.replace("'",'"')
                    string_to_list = json.loads(json_String)
                except Exception as e:
                    print(e)
            return string_to_list

#Logging Setup
logging.basicConfig(filename="ChatLogs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def chat_List_AI(Transcribed_text,question_list):
    global input_token_count
    global output_token_count
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat. The answers for each query will be a 'Yes' or 'No'"
            user_prompt = f"""
                        You will be provided with a conversation, which includes  Customer, Associate and Music,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat. The answers for each query will be a 'Yes' or 'No' or 'Not Applicable'
                        Task to perform:
                        -Provide ONLY the python list consisting of answers.
                        The question list is: ```{question_list}```
                        The transcribed text is:   ```{Transcribed_text}```
                        """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            
            
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                messages = [{"role":"system","content":system_prompt},
                        {"role":"user",
                        "content": user_prompt
                        }
                        ],
                temperature=0,
                max_tokens=800,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                stop=None
                )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list) 
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list)
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                try:
                    print('Trying Json Method to get answer')
                    json_String = test_list.replace("'",'"')
                    string_to_list = json.loads(json_String)
                except Exception as e:
                    print(e)
            return string_to_list




def second_chat_List_AI(Transcribed_text,question_list):
    global input_token_count
    global output_token_count
    
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat, Respond with only the subject complement noun, don't use subject and linking verbs"
            user_prompt = f"""
                        You will be provided with a conversation, which includes  Customer, Associate and Music,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat, Respond with only the subject complement noun, don't use subject and linking verbs
                        Task to perform:
                        -Provide ONLY the python list consisting of answers.
                        The question list is: ```{question_list}```
                        The transcribed text is:   ```{Transcribed_text}```
                        """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                messages = [{"role":"system","content":system_prompt},
                        {"role":"user",
                        "content":user_prompt
                        }
                        ],
                temperature=0.3,
                max_tokens=800,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                stop=None
                )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list) 
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list)
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                try:
                    print('Trying Json Method to get answer')
                    json_String = test_list.replace("'",'"')
                    string_to_list = json.loads(json_String)
                except Exception as e:
                    print(e)
            return string_to_list



#To get the issue category

def third_chat_List_AI(Transcribed_text, question_list):
    global input_token_count
    global output_token_count
    issue_category=['Login Issue / Incorrect User Information','Software Troubleshooting','Delayed Customer Service Response','IT Support']
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat"
            user_prompt = f"""
                    You will be provided with a conversation, which includes  Customer, Associate and Music,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat.
                    Task to perform:
                    -Provide ONLY the python list consisting of answers.
                    The question list is: ```{question_list}```
                    The transcribed text is:   ```{Transcribed_text}```
                    --Example Output
                    --["Answer1","Answer2",...]
                    """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
            messages = [{"role":"system","content": system_prompt},
                    {"role":"user",
                    "content":user_prompt
                    }
                    ],
                    temperature=1,
                    max_tokens=400,
                    top_p=0.95,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stop=None
                    )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            # test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list)  
            # test_list = re.search('\[.*\]', test_list).group(0)
            test_list = resp.choices[0].message.content
            #test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list)  
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list) 
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                print(e)
                try:
                        print('Trying Json Method to get answer')
                        #json_String = test_list.replace("'",'"')
                        json_String = re.sub(r"(?<!s)'(?!s)", '"', test_list)
                        print(json_String)
                        string_to_list = json.loads(json_String)
                except Exception as e:
                        print(e)
            return string_to_list





def chatFourthOpenAI(Transcribed_text):
    global input_token_count
    global output_token_count
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2024-02-01",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. Give Feedback along the following categories, chat Opening By Associate, If User Verfication is done by Associate, Was there hold by the associate, Did the Associate Empathize with the customer, chat ending by the associate. And at End provide some recommendations to the associate"
            user_prompt = f"{Transcribed_text}"
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                    messages = [{"role":"system","content": system_prompt},
                            {"role":"user","content": user_prompt}
                            ],
                    temperature=0.3,
                    max_tokens=800,
                    top_p=0.95,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stop=None
                    )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            output_token_count = output_token_count + len(encoding.encode(resp.choices[0].message.content))
            return resp.choices[0].message.content



#question list: chat Opening Score, Empathy Score, chat Hold Score, User Verification Score, chat Closing Score, Clear Communication Score, Overall chat Score
def fifth_chat_List_AI(feedback,question_list):
    global input_token_count
    global output_token_count
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat. You are supposed to provide a score (NUMERIC) for a particulat category betwen 0 - 10. based on the feedback and the topic given by the user"
            user_prompt = f"""
                        You will be provided with a conversation, which includes  Customer, Associate and Music,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed Chat. You are supposed to provide a score (NUMERIC) for a particulat category betwen 0 - 10. based on the feedback and the topic given by the user.
                        Task to perform:
                        -Provide ONLY the python list consisting of ONLY the numeric value for the score.
                        The feed back is ```{feedback}```
                        The question list: ```{question_list}```
                        """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                messages = [{"role":"system","content":system_prompt},
                        {"role":"user",
                        "content": user_prompt
                        }
                        ],
                temperature=0,
                max_tokens=800,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                stop=None
                )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list) 
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list)
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                try:
                    print('Trying Json Method to get answer')
                    json_String = test_list.replace("'",'"')
                    string_to_list = json.loads(json_String)
                except Exception as e:
                    print(e)
            return string_to_list

def sixth_chat_List_AI(Transcribed_text,question_list):
    global input_token_count
    global output_token_count
    
    while True:
        try:
            client = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
            api_version="2023-07-01-preview",
            api_key="ed65d8873b1d4365a48d53ef51c38413")
            system_prompt = f"You are a Quality Tester for Associate support chats, You will be provided with a conversation between Associate and Customer. You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed audio."
            user_prompt = f"""
                            You will be provided with a conversation, which includes  Customer, Associate ,You will be given a list of questions. You provide answer in a python list format for the corresponding questions provided in the question list based on the transcribed audio.
                            Task to perform:
                            -Provide ONLY the python list consisting of answers.
                            The question list is: ```{question_list}```
                            The transcribed text is:   ```{Transcribed_text}```
                            --Example Output
                            --["Answer1","Answer2",...]
                            """
            input_token_count = input_token_count + len(encoding.encode(system_prompt)) + len(encoding.encode(user_prompt))
            resp = client.chat.completions.create(model="kel-genops-sd-4o",
                    messages = [{"role":"system","content":system_prompt},
                            {"role":"user",
                            "content":user_prompt
                            }
                            ],
                    temperature=0.3,
                    max_tokens=800,
                    top_p=1,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stop=None
                    )
        except:
            print("delay 1 min")
            sleep(60)
        else:
            test_list = resp.choices[0].message.content
            #test_list = re.sub('[^A-Za-z0-9\[\]\'\, ]+', '', test_list) 
            test_list = re.search('\[.*\]', test_list).group(0)    
            print(test_list)
            output_token_count = output_token_count + len(encoding.encode(test_list))
            try:
                string_to_list = ast.literal_eval(test_list)
            except Exception as e:
                try:
                    print('Trying Json Method to get answer')
                    #json_String = test_list.replace("'",'"')
                    json_String = re.sub(r"(?<!s)'(?!s)", '"', test_list)
                    string_to_list = json.loads(json_String)
                except Exception as e:
                    print(e)
            return string_to_list
        
import pandas as pd
def start_Chat_analytics(file_path, username_needed, date_time, user_Chat_file_blob, id_for_error   ):
    df1 = pd.DataFrame(
        columns=[
            'Employee Name',
            'File Name',
            "Did Associate probe relevant questions & paraphrased to understand the issue correctly?",
            "Did the Associate display urgency, ownership and attempt to achieve first contact resolution? (Avoids unnecessary hand-off)",
            "Incase of additional time required to resolve the issue did the associate  keep the end-user informed about the status and delay?",
            "Did Associate use simple and professional language without sounding rude or casual ?",
            "Did Associate listen, acknowledge and empathize with the end-user appropriately throughout the chat where necessary ?",
            "Was Associate Confident, conversational, convincing and proactive (Rapport building e.g: referring the user with their name)",
            "Did Associate manage dead air/time appropriately during Chat?",
            "Did Associate follow hold procedure appropriately for Chat? (Followed the script)",
            "Did the Associate display confidence and also tactfully manage service denial with appropriate workarounds? (For requests out of SD scope)",
            "Did the Associate check for additional help and  use appropriate chat closure statement (Avoids abrupt chat/chat closure)",
            "What was the sentiment of the user towards the issue resolution? (User experience)",
            "What was Reason the sentiment of the user towards the issue resolution? (User experience)",
            "What was the sentiment of the user towards the agent's assistance and professionalism?",
            "What was Reason the sentiment of the user towards the agent's assistance and professionalism?"
        ]
    )
    Chat_files_in_list = file_path
        
    # conn = pyodbc.connect('DRIVER=' + Driver + ';SERVER=' +
    #     Server + ';PORT=1433;DATABASE=' + Database +
    #     ';UID=' + Uid + ';PWD=' + Pwd)
    # cursor = conn.cursor()
    # sql_query = "INSERT INTO [Kelly_Resources].[RPT_Chat_Analytics_Report_table_V1] (user_name, time_of_upload, Employee_Name, File_Name, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11_Sentiment, Q12_Sentiment_Reason, Issue, Resolution, Feedback, chat_Operning_Score, Emapthy_Score, chat_Hold_Score, User_Verification_Score, chat_closing_Score, Clear_Communication_Score, Overall_chat_Score, Issue_category,chat_Quality_Score,chat_Quality,chat_Quality_Review) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    values_list = []
    values_list.append(file_path)
    #values_list.append(date_time)
    #Queries
    query_list = []
    query_list.append("Did Associate probe relevant questions & paraphrased to understand the issue correctly?")
    query_list.append("Did the Associate display urgency, ownership and attempt to achieve first contact resolution? (Avoids unnecessary hand-off)")
    query_list.append("Incase of additional time required to resolve the issue did the associate  keep the end-user informed about the status and delay?")
    query_list.append("Did Associate use simple and professional language without sounding rude or casual ?")
    query_list.append("Did Associate listen, acknowledge and empathize with the end-user appropriately throughout the chat/chat where necessary ?")
    query_list.append("Was Associate Confident, conversational, convincing and proactive (Rapport building e.g: referring the user with their name)")
    query_list.append("Did Associate manage dead air/time appropriately during chat/Chat?")
    query_list.append("Did Associate follow hold procedure appropriately for chat/Chat? (Followed the script) ")
    query_list.append("Did the Associate display confidence and also tactfully manage service denial with appropriate workarounds? (For requests out of SD scope) ")
    query_list.append("Did the Associate check for additional help and  use appropriate chat/chat closure statement (Avoids abrupt chat/chat closure) ")
    second_query_list = []
    sixth_query_list = []
    second_query_list.append("What is the Name of the Associate?")
    #second_query_list.append("What was the sentiment of the user chat? (User experience). Tell Positive or Neutral or Negative.")
    second_query_list.append("Based on the context of the conversation, can you determine the user's overall sentiment during the conversation?. Tell Positive or Neutral or Negative")
    #second_query_list.append("What was the sentiment of the user chat? (User experience). Tell Positive or Neutral or Negative. also mention why the tone is positive or neutral or negative.")
    #second_query_list.append("Based on the context of the conversation, can you determine the user's overall sentiment during the conversation?. Tell Positive or Neutral or Negative. State the reason on why the tone is positive or neutral or negative."
    sixth_query_list.append("Based on the context of the conversation, can you provide some insight on the user's overall sentiment during the conversation?.")
    second_query_list.append("From the conversation context, can you infer the user's sentiment towards the agent's assistance and professionalism?. Tell Positive or Neutral or Negative.")
    #second_query_list.append("From the conversation context, can you infer the user's sentiment towards the agent's assistance and professionalism?. Tell Positive or Neutral or Negative. State the reason on why the tone is positive or neutral or negative.")
    sixth_query_list.append("From the conversation context, can you provide some insight on the user's sentiment towards the agent's assistance and professionalism?.")


#     Chat = ChatSegment.from_file(file_path)
#     chunk_length_ms = 60000 * 4
#     chunks = make_chunks(Chat, chunk_length_ms)

    
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    if username_needed == 'blobuser@mail.com':
        container_name = 'chat-input'
    else:
        container_name = 'chat-input-gui'
    #container_client = blob_service_client.create_container(container_name)
# # #     blob_client = blob_service_client.get_blob_client(container=container_name, blob=user_Chat_file_blob)
# # #     with open(file=user_Chat_file_blob, mode="wb") as download_file:
# # #         download_file.write(blob_client.download_blob().readall())  
    # download blob content to a temporary file  
  
    # your chunking and transcription code here, using `wave` with the temporary file 
    #full_transcription = get_trancription(user_Chat_file_blob, f'{username_needed}_{id_for_error}_{date_time}') 
    #dataframe = pd.read_csv(file_path)
    
    full_transcription = dataframe_to_string(file_path)
    
    #print(full_transcription)
    # delete the temporary file when done  
    #os.remove(user_Chat_file_blob)  
#     for i, chunk in enumerate(chunks):
#         print(f"Transcribing chunk {i+1}/{len(chunks)}")
#         chunk_transcription = transcribe_Chat_chunk(chunk, i,)
#         full_transcription += chunk_transcription + "\n"
    result_list = []
    result_second_list = []
    result_list = chat_List_AI(full_transcription, query_list)
    result_list_score = chat_List_First_Score_AI(full_transcription, query_list)
    result_score_weightage = fromListToScore_NA(result_list)
    overall_weighted_score = 0
    #print(f'{len(result_list_score)}: {len(result_score_weightage)}')
    individual_score_llm_list = []
    individual_score_llm_string_list = []
    for x in range(0, len(result_list_score)):
        #print(x)
        if result_score_weightage[x] != 'NA':
            individual_score_llm_list.append((result_list_score[x]/10) * result_score_weightage[x])
            individual_score_llm_string_list.append(f"{round(individual_score_llm_list[x],2)} / {round(result_score_weightage[x],2)}")
            overall_weighted_score = overall_weighted_score + (individual_score_llm_list[x])
        else:
            individual_score_llm_list.append(0)
            individual_score_llm_string_list.append('NA')
    #for x in range(0, len(individual_score_llm_list)):
        
    #print(result_list)
    overall_weighted_score = round(overall_weighted_score, 2)
    print(f'Overall Weighted Score: {overall_weighted_score}')
    result_second_list= second_chat_List_AI(full_transcription, second_query_list)
    result_sixth_list = []
    result_sixth_list= sixth_chat_List_AI(full_transcription, sixth_query_list)
    list=[result_second_list[0],user_Chat_file_blob,result_list[0],result_list[1],result_list[2],result_list[3],result_list[4],result_list[5],result_list[6],result_list[7],result_list[8],result_list[9],result_second_list[1],result_sixth_list[0], result_second_list[2], result_sixth_list[1]]
    values_list.extend(list)

    print('Step 1')
    df1.loc[0] = list
    issue_category=['Login Issue / Incorrect User Information','Software Troubleshooting','Delayed Customer Service Response','IT Support']
    third_query = ["What is the Issue based on the conversation?", "What is the resolution arrived?", f"Choose ONE issue from the category that fits issue. The issue categories: {issue_category}"]
    # issue_list = []
    # resolution_list = []
    result_third_list = []
    feedback_list = []
    result_third_list=third_chat_List_AI(full_transcription, third_query)  #14 is Trranscript
    print('Step 2')
    response1 = chatFourthOpenAI(full_transcription)
    feedback_list.append(response1)
    print('Step 3')
    df1['Issue Category'] = [result_third_list[2]]
    df1['Issue'] = [result_third_list[0]]
    df1['Resolution'] = [result_third_list[1]]
    df1['Feedback'] = feedback_list
    values_list.append(result_third_list[0])
    values_list.append(result_third_list[1])
    values_list.extend(feedback_list)
    
    #question list: chat Opening Score, Empathy Score, chat Hold Score, User Verification Score, chat Closing Score, Clear Communication Score, Overall chat Score
    fifth_query = ['Give Score for Associates chat Opening?', 'Give Score for Associates Empathy?', 'Give Score for Associates chat Hold Handling, with exception being if no hold is there in conversation give more marks?', 'Give Score for Associates chat Closing?', 'Give Score for Associates Clear Communication?', 'What is the Overall chat Score?']
    #output_file_name='./Output/user_1.csv'
    result_fifth_list = []
    result_fifth_list = fifth_chat_List_AI(feedback_list[0], fifth_query)
    #df1.to_csv(output_file_name)
    df1['chat Opening  Score'] = result_fifth_list[0]
    df1['Empathy Score'] = result_fifth_list[1]
    df1['chat Hold Score'] = result_fifth_list[2]
    #df1['User Verification Score'] = result_fifth_list[3]
    df1['chat Closing Score'] = result_fifth_list[3]
    df1['Clear Communication Score'] = result_fifth_list[4]
    values_list.append(result_fifth_list[0])
    values_list.append(result_fifth_list[1])
    values_list.append(result_fifth_list[2])
    values_list.append(result_fifth_list[3])
    values_list.append(result_fifth_list[4])
    #values_list.append(result_fifth_list[5])
    
    df1['Overall chat Score'] = result_fifth_list[5]
    values_list.append(result_fifth_list[5])
    values_list.append(result_third_list[2])
    #Adding Additonal Code for chat Quality and Score

    def chat_quality(empathy,hold,clear_communication):

        chat_quality_score=(empathy+hold+clear_communication)/3
        chat_quality=''
        reason=''

        if chat_quality_score<7:
                chat_quality='Poor'
        elif chat_quality_score>=7 and chat_quality_score<=8:
                chat_quality='Average'
        else:
                chat_quality='Good'


        if chat_quality_score<7:
                reason=reason+'The overall quality of the chat based on the score is Poor. '
        elif chat_quality_score>=7 and chat_quality_score<=8:
                reason=reason+'The overall quality of the chat based on the score is Average. '
        else:
                reason=reason+'The overall quality of the chat based on the score is Good. '
        if empathy<7:
                reason=reason+'The agent lacked empathy in the conversation. '
        elif empathy>=7 and empathy<=8:
                reason=reason+'The agent had moderate empathy in the conversation. '
        else:
                reason=reason+'The agent demonstrated strong empathy in the conversation. '
        
        if hold<7:
                reason=reason+'Frequent hold times were noted, indicating a need for better preparation and resource management. '
        elif hold>=7 and hold<=8:
                reason=reason+'Hold times were acceptable. '
        else:
                reason=reason+'Hold times were minimal, showing efficient handling of the chat. '

        if clear_communication<7:
                reason=reason+'Communication clarity was poor. '
        elif clear_communication>=7 and clear_communication<=8:
                reason=reason+'Communication was generally clear but could have been better. '
        else:
                reason=reason+'Communication was clear. '

        return chat_quality_score, chat_quality, reason


    df1['chat_Quality_Score']=0.0
    df1['chat_Quality']=''
    df1['chat_Quality_Review']=''
    for i in range(len(df1)):
        empathy_score=df1['Empathy Score'][i]
        chat_hold_score=df1['chat Hold Score'][i]
        clear_comm_score=df1['Clear Communication Score'][i]

        try:
                empathy_score=df1['Empathy Score'][i].astype(float)
        except:
                empathy_score=0

        try:
                chat_hold_score=df1['chat Hold Score'][i].astype(float)
        
        except:
                chat_hold_score=0

        try:
                clear_comm_score=df1['Clear Communication Score'][i].astype(float)
        except:
                clear_comm_score=0

        quality_score,quality,reason=chat_quality(empathy_score,chat_hold_score,clear_comm_score)
        df1['chat_Quality_Score'][i]=quality_score
        df1['chat_Quality'][i]=quality
        df1['chat_Quality_Review'][i]=reason

    values_list.append(df1['chat_Quality_Score'][0])
    values_list.append(df1['chat_Quality'][0])
    values_list.append(df1['chat_Quality_Review'][0])

    df1['Question1'] = individual_score_llm_string_list[0]
    df1['Question2'] = individual_score_llm_string_list[1]
    df1['Question3'] = individual_score_llm_string_list[2]
    df1['Question4'] = individual_score_llm_string_list[3]
    df1['Question5'] = individual_score_llm_string_list[4]
    df1['Question6'] = individual_score_llm_string_list[5]
    df1['Question7'] = individual_score_llm_string_list[6]
    df1['Question8'] = individual_score_llm_string_list[7]
    df1['Question9'] = individual_score_llm_string_list[8]
    df1['Question10'] = individual_score_llm_string_list[9]
    df1['Overall_Weighted_Score'] = f"{overall_weighted_score} / {100.0}"



    #Code


    # if len(values_list) == 30:
    #     #print('Need to Add to Database')
    #     value_tuple = tuple(values_list)
    #     cursor.execute(sql_query, value_tuple)
    #     conn.commit()
    #     # Close the cursor and connection
    #     cursor.close()
    #     conn.close()
    # else:
    #     print('ERROR during Score inserting to Database')
    #     cursor.close()
    #     conn.close()
    return df1


username_needed = 'temp'
user_Chat_file_blob = 'temp'
id_for_error = 'temp'
def finalChatProcess(username_needed1, user_Chat_file_blob1, id_for_error1):
    global username_needed
    username_needed = username_needed1
    global user_Chat_file_blob
    user_Chat_file_blob = user_Chat_file_blob1
    global id_for_error
    id_for_error = id_for_error1
    user_folder = './Output/' +username_needed
    now = datetime.now() # current date and time
    date_time = now.strftime("%d%m%Y%H%M%S")
    fileName_csv = user_folder+ '/Chat_Analytics_Report_'+username_needed + id_for_error +'_' + date_time + '.csv'
    fileName_excel = user_folder+ '/Chat_Analytics_Report_'+username_needed + id_for_error +  '_' + date_time + '.xlsx'

    try:
        #Arguments to be passed into python
        logger.info(f"{username_needed}'s {user_Chat_file_blob} Processing Started")
        print(f'Process Started for {user_Chat_file_blob}')
        blob_service_client = BlobServiceClient(account_url, credential=default_credential)
        if username_needed == 'blobuser@mail.com':
            container_client = blob_service_client.get_container_client(container= 'chat-input')
        else:
            container_client = blob_service_client.get_container_client(container= 'chat-input-gui')
        print("\nFetch Data from Blob\n\t")
        blob_name = user_Chat_file_blob
        user_Chat_data = container_client.download_blob(blob_name).readall()
        import io
        user_Chat_file_path = io.BytesIO(user_Chat_data)

        """
        blob table -> user upload file to blob -> filename, status -> blob table -> Status - Pending
        fetch all files that are pending to list and status to start
        after process and add report to database change 
        func(user, file):

        """
        #File Path to Blob Fetch




        

        

        if not os.path.exists(user_folder):
            os.makedirs(user_folder)
            print(f"Folder '{username_needed}' created")
        else:
            print(f"Folder '{username_needed}' exists")


        
        #Chat_list=[os.path.join(user_Chat_file_list,f) for f in os.listdir(user_Chat_file_list) if f.endswith('.WAV')]
        
        Chat_list=[]
        Chat_list.append(user_Chat_file_path)
        Chat_df_list=[]
        i=0
        
        for Chat_f in Chat_list:
            
            df_name='df_'+str(i)
            df_name=start_Chat_analytics(Chat_f, username_needed, date_time, user_Chat_file_blob, id_for_error)
            
            Chat_df_list.append(df_name)
        combined_df=pd.concat(Chat_df_list,ignore_index=True)

        #Adding code for additional scoring and call quality Review
        combined_df.to_csv(fileName_csv,index=False)
        
        UTC = pytz.utc
        IST = pytz.timezone('Asia/Kolkata')
        now1 = datetime.now(IST)
        date_time1 = now1.strftime("%d/%m/%Y,%H:%M:%S IST")
        #To excel
        #combined_df.to_excel(fileName_excel)
        #To Formatted Excel -> Needs template file -> .xtlx
        wb = openpyxl.load_workbook('./Templates/Template_Chat_Analytics_With_GenAI_V1.xltx')
        ws = wb.active
        
        # Convert the dataframe into rows
        rows = dataframe_to_rows(combined_df, index=False, header=False)
        
        # Write the rows to the worksheet
        for r_idx, row in enumerate(rows, 3):
            #print(row)
            for c_idx, value in enumerate(row, 1):
                #print(value)
                #print('debug0')
                ws.cell(row=r_idx, column=c_idx, value=value)
        # Save the worksheet as a (*.xlsx) file
        
        wb.template = False
        #print('debug1')
        wb.save(fileName_excel)
        try:
            blob_service_client = BlobServiceClient(account_url, credential=default_credential)
            container_name = "chat-output"
            container_client = blob_service_client.create_container(container_name)
        except Exception as e:
            print(e)
        local_path = user_folder
        local_file_name = 'Chat_Analytics_Report_'+username_needed + id_for_error +  '_' + date_time + '.xlsx'
        upload_file_path = os.path.join(local_path, local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
        #print('debug2')
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
        #print('debug3')
    #     with open(file=upload_file_path, mode="rb") as data:
    #         blob_client.upload_blob(data)
        conn = pyodbc.connect('DRIVER=' + Driver + ';SERVER=' +
            Server + ';PORT=1433;DATABASE=' + Database +
            ';UID=' + Uid + ';PWD=' + Pwd)
        cursor = conn.cursor()
        if username_needed == 'blobuser@mail.com':
            sql_query1 = f"UPDATE [Kelly_Resources].[Chat_Analytics_Blob_Files_V2] SET [status] = 'Completed' WHERE [Rowid_object] ={id_for_error}"
        else:
            sql_query1 = f"UPDATE [Kelly_Resources].[Chat_Analytics_Blob_Files_GUI_V1] SET [status] = 'Completed' WHERE [Rowid_object] ={id_for_error}"
        sql_query2 = "INSERT INTO [Kelly_Resources].[Chat_Analytics_ReportpathDetails] (Email, Timestamp, StoragePath, blobname, container) VALUES (?, ?, ?, ?, ?)"
        values_query2 = (username_needed, date_time1, fileName_excel, local_file_name, container_name)
        cursor.execute(sql_query1)
        cursor.execute(sql_query2, values_query2)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"{username_needed}'s {user_Chat_file_blob} Processing Completed")
        print(f'Process Succeeded for {user_Chat_file_blob}')
        if os.path.exists(fileName_csv):  
                # Remove the file  
            os.remove(fileName_csv) 
        return "True"
    except Exception as e:
        # conn = pyodbc.connect('DRIVER=' + Driver + ';SERVER=' +
        #     Server + ';PORT=1433;DATABASE=' + Database +
        #     ';UID=' + Uid + ';PWD=' + Pwd)
        # cursor = conn.cursor()
        # if username_needed == 'blobuser@mail.com':
        #     sql_query1 = f"UPDATE [Kelly_Resources].[Chat_Analytics_Blob_Files_V2] SET [status] = 'Pending' WHERE [Rowid_object] ={id_for_error}"
        # else:
        #     sql_query1 = f"UPDATE [Kelly_Resources].[Chat_Analytics_Blob_Files_GUI_V1] SET [status] = 'Pending' WHERE [Rowid_object] ={id_for_error}"
        # cursor.execute(sql_query1)
        # conn.commit()
        # cursor.close()
        # conn.close()
        # logger.info(f"{username_needed}'s {user_Chat_file_blob} Processing Changed to Pending")
         
        print(f'Process Failed for {user_Chat_file_blob} added back to Pending')
        if os.path.exists(fileName_csv):  
            os.remove(fileName_csv)

        print(e)
        return e


#finalChatProcess('rane.ajay@tcs.com', 'rane.ajay@tcs.com_chat1_transcript_test_v1.pdf', '2')