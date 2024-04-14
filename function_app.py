import azure.functions as func
import logging
import pyodbc #for sql connections 
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from flask import jsonify  #package in order to use json , return json to client for example

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'

# Function to create a new case in the 'cases' table
def create_case_in_database(casename):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute("INSERT INTO cases (name, status,userid) VALUES (?, ?, ?)", (casename, 1,1))
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

# Define the Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="v1/case/create", methods=['POST'])
def create_case(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for creating a case.')
    # Extract casename from the request
    casename = req.params.get('casename')
    # Check if casename is provided
    if not casename:
        return func.HttpResponse("Parameter 'casename' is missing in the request.", status_code=400)
    case_id = create_case_in_database(casename)
    if case_id is not None:
        logging.info(f"case_id contains data , the value is:{case_id}")
        #return func.HttpResponse(f"Case {case_id} created successfully.", status_code=200)
        data = { 
            "caseid" : case_id, 
            "Subject" : "Case created successfully" 
        } 
        jsonResult = jsonify(data)
        return func.HttpResponse(jsonResult, status_code=200)
    else:
        return func.HttpResponse("Failed to create case.", status_code=500)

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