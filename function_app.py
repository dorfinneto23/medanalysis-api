import azure.functions as func
import logging
import pyodbc #for sql connections 
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
import json # in order to use json 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')
#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'

# Function to create a new case in the 'cases' table
def create_case_in_database(casename,userid):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute("INSERT INTO cases (name, status,userid) VALUES (?, ?, ?)", (casename, 1,userid))
        conn.commit()

        # Get the ID of the last inserted row
        cursor.execute("SELECT @@IDENTITY AS 'Identity';")
        case_id = cursor.fetchone()[0]

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info("New case created successfully in the 'cases' table.")
        return case_id
    except Exception as e:
        logging.error(f"Error creating case: {str(e)}")
        return None
    
# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? WHERE id = ?", (value, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    

# Function to upload a PDF file to Azure Blob Storage
def upload_to_blob_storage(file_stream, filename,caseid):
    try:
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        path = f"{basicPath}/source/{filename}"
        #check if file Exists
        blob_get = container_client.get_blob_client(path)
        fileExist = blob_get.exists()
        logging.info(f"fileExist value is: {fileExist}")
        if fileExist==True:
            return "fileExist"
         #Upload the file to Azure Blob Storage
        logging.info(f"before upload file, path value: {path}")
        blob_client = container_client.upload_blob(name=path, data=file_stream)
        logging.info(f"file uploaded succeeded")
        logging.info(f"blob file url is: {blob_client.url}")
        if not blob_client.url:
           return "uploadfailed"
        else: 
           update_case_generic(caseid,"path",basicPath)
           create_servicebus_event("ocr",caseid)
           return "uploaded"
    except Exception as e:
        return str(e)
    
#Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

# Define the Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="v1/case/create", methods=['POST'])
def create_case(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for creating a case.')
    # Extract casename & userid from the request
    casename = req.params.get('casename')
    userid = req.params.get('userid')
    # Check if casename is provided
    if not casename:
        return func.HttpResponse("Parameter 'casename' is missing in the request.", status_code=400)
    case_id = create_case_in_database(casename,userid)
    if case_id is not None:
        logging.info(f"case_id contains data , the value is:{case_id}")
        # prepare json data
        case_id_int = int(case_id)
        data = { 
            "caseid" : case_id_int, 
            "Subject" : "Case created successfully!" 
        } 
        json_data = json.dumps(data)
        return func.HttpResponse(body=json_data, status_code=200,mimetype="application/json")
    else:
        return func.HttpResponse("Failed to create case.", status_code=500)
    
@app.route(route="v1/case/uploadfile", methods=['POST'])
def upload_pdf(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('starting upload file request')
    # Extract caseid from the request
    caseid = req.params.get('caseid')
    if not caseid:
        return func.HttpResponse("Parameter 'caseid' is missing in the request.", status_code=400)
    try:
        # Check if file is included in the request
        if 'file' not in req.files:
            return func.HttpResponse("No file provided in the request.", status_code=400)

        file = req.files['file']
        file_name = file.filename
        
        # Upload the file to Azure Blob Storage
        uploadtatus = upload_to_blob_storage(file, file_name,caseid)

        if uploadtatus == "fileExist":
            data = { 
            "status" : "fileExist", 
            "Description" : "File Exist if you wish to replace it , use another api request for file replacment" 
             } 
            json_data = json.dumps(data)
            return func.HttpResponse(body=json_data, status_code=200,mimetype="application/json")
        elif uploadtatus=="uploaded":
            #update case status = 2 (uploaded)
            updatestatus = update_case_generic(caseid,"status",2)
            data = { 
            "status" : "uploaded", 
            "Description" : f"File uploaded successfully and case status updated to: {updatestatus} " 
             } 
            json_data = json.dumps(data)
            return func.HttpResponse(body=json_data, status_code=200,mimetype="application/json")
        elif uploadtatus=="uploadfailed":
            #update case status = 3 (Upload failed)
            updatestatus = update_case_generic(caseid,"status",3) 
            data = { 
            "status" : "uploadfailed", 
            "Description" : f"File uploaded failed and case status updated to: {updatestatus} " 
             } 
            json_data = json.dumps(data)
            return func.HttpResponse(body=json_data, status_code=500,mimetype="application/json")
        else:
            #update case status = 3 (Upload failed)
            updatestatus =  update_case_generic(caseid,"status",3) 
            data = { 
            "status" : "uploadfailed", 
            "Description" : f"Failed to upload file - Unexpected error and case status updated to: {updatestatus} " 
             } 
            json_data = json.dumps(data)
            return func.HttpResponse(body=json_data, status_code=500,mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
    
@app.route(route="v1")
def v1(req: func.HttpRequest) -> func.HttpResponse:
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