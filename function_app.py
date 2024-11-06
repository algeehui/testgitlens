import azure.functions as func
import datetime
import json
import logging
import os

app = func.FunctionApp()

@app.route(route="HelloWorld", auth_level=func.AuthLevel.ANONYMOUS)
def HelloWorld(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    f_name = req.params.get('fname')
    l_name = req.params.get('lname')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if f_name and l_name:
        return func.HttpResponse(f"Hello, {f_name} {l_name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. My name is {os.getenv('MyName')}.",
             status_code=200
        )