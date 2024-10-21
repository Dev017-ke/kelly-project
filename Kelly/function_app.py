import azure.functions as func
import datetime
import json
import logging
from audio_process import finalAudioProcess
from chat_process import finalChatProcess
from kb_process import finalKBprocess
from kb_embed import finalKBembed
from trendingQueries import trendingQueryInit
app = func.FunctionApp()
from time import sleep
Driver='{ODBC Driver 18 for SQL Server}'
Server='tcp:atubsu-hranalyticsserver.database.windows.net,1433'
Database='hranalyticsdb'
Uid='HRadmin'
Pwd='Welcome#2021'
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient
from io import StringIO
import pandas as pd
import pyodbc
from openai import AzureOpenAI

@app.route(route="audioHTTPTrigger", auth_level=func.AuthLevel.FUNCTION)
def audioHTTPTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    username = req.params.get('username')
    filename = req.params.get('filename')
    local_id = req.params.get('id')
    if not username:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            username = req_body.get('username')
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get('filename')
    if not local_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            local_id = req_body.get('id')
            

    if username and filename and local_id:
        run = finalAudioProcess(username, filename, local_id)
        if run == 'True':
            print('Report Generated')
        else:
            print(run)
            print('Report Generation Failed')
        return func.HttpResponse(f"Hello, {username}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass all required params in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.route(route="chatHTTPTrigger", auth_level=func.AuthLevel.FUNCTION)
def chatHTTPTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    username = req.params.get('username')
    filename = req.params.get('filename')
    local_id = req.params.get('id')
    if not username:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            username = req_body.get('username')
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get('filename')
    if not local_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            local_id = req_body.get('id')
            

    if username and filename and local_id:
        run = finalChatProcess(username, filename, local_id)
        if run == 'True':
            print('Report Generated')
        else:
            print(run)
            print('Report Generation Failed')
        return func.HttpResponse(f"Hello, {username}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass all required params in the query string or in the request body for a personalized response.",
             status_code=200
        )



@app.route(route="kbHTTPTrigger", auth_level=func.AuthLevel.FUNCTION)
def kbHTTPTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    username = req.params.get('username')
    filename = req.params.get('filename')
    local_id = req.params.get('id')
    if not username:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            username = req_body.get('username')
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get('filename')
    if not local_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            local_id = req_body.get('id')
            

    if username and filename and local_id:
        run = finalKBprocess(username, filename, local_id)
        if run == 'True':
            print('Report Generated')
        else:
            print(run)
            print('Report Generation Failed')
        return func.HttpResponse(f"Hello, {username}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass all required params in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.queue_trigger(arg_name="azqueue", queue_name="testing-function-app-kelly",
                               connection="AzureWebJobsStorage") 
def kbQueueTrigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))
    queue_content = azqueue.get_body().decode('utf-8')
    try:
        data = json.loads(queue_content)
        #print(data) 
        username = data['username']  
        filename = data['filename']
        local_id = data['id']
        print(username)
        print(filename)
        print(local_id)
        sleep(10)
        if username and filename and local_id:
            run = finalKBprocess(username, filename, local_id)
            if run == 'True':
                print('Report Generated')
            else:
                print(run)
                print('Report Generation Failed')
            print(f"Hello, {username}. Your File {filename} in {local_id} is Processed.")
        else:
            print(f"Hello, {username}. Your File {filename} in {local_id} is Failed.")
    except json.JSONDecodeError as e:  
        logging.error(f'Error parsing JSON: {e}') 
    except Exception as e:  
        logging.error(f'Unexpected error: {e}')  



@app.queue_trigger(arg_name="azqueue", queue_name="testing-chat-queue",
                               connection="AzureWebJobsStorage") 
def chatQueueTrigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))
    queue_content = azqueue.get_body().decode('utf-8')
    try:
        data = json.loads(queue_content)
        #print(data) 
        username = data['username']  
        filename = data['filename']
        local_id = data['id']
        print(username)
        print(filename)
        print(local_id)
        sleep(10)
        if username and filename and local_id:
            run = finalChatProcess(username, filename, local_id)
            if run == 'True':
                print('Report Generated')
            else:
                print(run)
                print('Report Generation Failed')
            print(f"Hello, {username}. Your File {filename} in {local_id} is Processed.")
        else:
            print(f"Hello, {username}. Your File {filename} in {local_id} is Failed.")
    except json.JSONDecodeError as e:  
        logging.error(f'Error parsing JSON: {e}') 
    except Exception as e:  
        logging.error(f'Unexpected error: {e}')  



@app.queue_trigger(arg_name="azqueue", queue_name="testing-audio-queue",
                               connection="AzureWebJobsStorage") 
def audioQueueTrigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))
    queue_content = azqueue.get_body().decode('utf-8')
    try:
        data = json.loads(queue_content)
        #print(data) 
        username = data['username']  
        filename = data['filename']
        local_id = data['id']
        print(username)
        print(filename)
        print(local_id)
        sleep(10)
        if username and filename and local_id:
            run = finalAudioProcess(username, filename, local_id)
            if run == 'True':
                print('Report Generated')
            else:
                print(run)
                print('Report Generation Failed')
            print(f"Hello, {username}. Your File {filename} in {local_id} is Processed.")
        else:
            print(f"Hello, {username}. Your File {filename} in {local_id} is Failed.")
    except json.JSONDecodeError as e:  
        logging.error(f'Error parsing JSON: {e}') 
    except Exception as e:  
        logging.error(f'Unexpected error: {e}')  



@app.queue_trigger(arg_name="azqueue", queue_name="testing-embed-queue",
                               connection="AzureWebJobsStorage") 
def kbEmbedQueueTrigger(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))
    queue_content = azqueue.get_body().decode('utf-8')
    try:
        data = json.loads(queue_content)
        #print(data) 
        username = data['username']  
        filename = data['filename']
        local_id = data['id']
        print(username)
        print(filename)
        print(local_id)
        sleep(10)
        if username and filename and local_id:
            run = finalKBembed(username, filename, local_id)
            if run == 'True':
                print('Embedding is Done')
            else:
                print(run)
                print('Embedding Failed')
            print(f"Hello, {username}. Your File {filename} in {local_id} is Processed.")
        else:
            print(f"Hello, {username}. Your File {filename} in {local_id} is Failed.")
    except json.JSONDecodeError as e:  
        logging.error(f'Error parsing JSON: {e}') 
    except Exception as e:  
        logging.error(f'Unexpected error: {e}') 


# @app.timer_trigger(schedule="0 30/60 11-23 * * 1-5", arg_name="myTimer", run_on_startup=False,
#               use_monitor=False) 
@app.route(route="timerTrendingQueries", auth_level=func.AuthLevel.FUNCTION)
def timerTrendingQueries(req: func.HttpRequest) -> func.HttpResponse:
#def timerTrendingQueries(myTimer: func.TimerRequest) -> None:
    # if myTimer.past_due:
    #     logging.info('The timer is past due!')
    try:
        client_oai = AzureOpenAI(azure_endpoint="https://kel-genops-sd.openai.azure.com/",
        api_version="2024-02-15-preview",
        api_key="ed65d8873b1d4365a48d53ef51c38413")
        account_url_tq = "https://kellygenaiblob.blob.core.windows.net"
        default_credential_tq = 'iHw9/Wcid+aMCGXCcWkQq6yqhhjUzcrLn0YonYiFkKzR86wZYfw40GGO6X833xT/xe2GQJtX8HzB+AStRG9++g=='
        blob_service_client_tq = BlobServiceClient(account_url_tq, credential=default_credential_tq)
        blob_client = blob_service_client_tq.get_blob_client(container = 'trending-query-asset', blob = 'Application_Names_Kelly_V1.csv')
        blob_data = blob_client.download_blob().content_as_text()
        csv_data = StringIO(blob_data)
        df_Application = pd.read_csv(csv_data)
        conn = pyodbc.connect('DRIVER=' + Driver + ';SERVER=' +
            Server + ';PORT=1433;DATABASE=' + Database +
            ';UID=' + Uid + ';PWD=' + Pwd)
        cursor = conn.cursor()
        last_n_queries = 100
        sql_query = f"SELECT * FROM (SELECT * FROM [Kelly_Resources].[Trending_Queries_Log_V1] ORDER BY Rowid_object DESC OFFSET 0 ROWS FETCH NEXT {last_n_queries} ROWS ONLY) AS Last100 ORDER BY Rowid_object ASC; "
        cursor.execute(sql_query)
        input_list = list(cursor.fetchall())
        #for id, log_date, status in input_list:
        queries_specific = [row.query for row in input_list] 
        oai_model = 'kel-genops-sd-4o'
        trend_queries = trendingQueryInit(df_Application, client_oai, oai_model,queries_specific)
        logging.info(trend_queries)
        logging.info('Python timer trigger function executed.')
        return func.HttpResponse(f"Hello. This HTTP triggered function executed successfully.")
    except Exception as e:  
        logging.error(f'Unexpected error: {e}') 

@app.route(route="func01", auth_level=func.AuthLevel.ANONYMOUS)
def func01(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )