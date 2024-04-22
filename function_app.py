import azure.functions as func
import logging
import pyodbc #for sql connections 
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
import json # in order to use json 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

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

# Function to update case status in the 'cases' table
def update_case_status(caseid,statusid):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute("UPDATE cases SET status = ? WHERE id = ?", (statusid, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info("case status updated")
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
        
        # Upload the file to Azure Blob Storage
        blob_client = container_client.upload_blob(name=f"{main_folder_name}/{folder_name}/{filename}", data=file_stream)
        logging.info(f"file uploaded succeeded: {blob_client.ErrorCode}")
        return blob_client.url
    except Exception as e:
        return str(e)

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
        blob_url = upload_to_blob_storage(file, file_name,caseid)

        if blob_url:
            #update case status = 2 (uploaded)
            updatestatus = update_case_status(caseid,2)
            return func.HttpResponse(f"File uploaded successfully. Blob URL: {blob_url} and case status updated {updatestatus}", status_code=200)
        else:
            #update case status = 3 (Upload failed)
            updatestatus = update_case_status(caseid,3)
            return func.HttpResponse("Failed to upload file to Azure Blob Storage.", status_code=500)
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