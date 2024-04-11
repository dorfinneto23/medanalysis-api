import azure.functions as func
import logging
import pyodbc



# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = 'sysadmin'
password = 'Qaz1057!@#'
driver= '{ODBC Driver 17 for SQL Server}'

# Function to create a new case in the 'cases' table
def create_case_in_database(casename):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute("INSERT INTO cases (name, status) VALUES (?, ?)", (casename, 1))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info("New case created successfully in the 'cases' table.")
        return True
    except Exception as e:
        logging.error(f"Error creating case: {str(e)}")
        return False

# Define the Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="v1/case/create", methods=['POST'])
def create_case(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for creating a case.')
    # Extract casename from the request
    casename = req.params.get('casename')
    # Check if casename is provided
    if not casename:
        return func.HttpResponse("Parameter 'casename' is missing in the request.", status_code=400)
    
    # Create the case in the database
    if create_case_in_database(casename):
        return func.HttpResponse("Case created successfully.", status_code=200)
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