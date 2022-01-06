from flask import Flask, request
from setup import init
from middleware import validate_authorization
import json
import requests
import os
import time
import re


app = Flask(__name__)

MAX_BATCH_SIZE, MAX_PARALLEL_BATCHES, MAX_RECORDS_PER_SECOND = 1, 1, 1

ENDPOINTS, OPERATION_MAP, FIELD_MAP = ['campaign-event'], {'campaign-event': ['append']}, {'campaign-event': [{'name': 'email', 'type': 'string'}]}


@app.route('/')
def validate_token():
    if validate_authorization(request):
        return 'Welcome to microservice'
    else:
        return 'Not authenticated'

def list_objects():
    result_objects = []
    for object_api_name in ENDPOINTS:
        # Need to change as you want to display this object within Census
        result_objects.append({"object_api_name": object_api_name, "label": object_api_name})
    return result_objects

def get_fields(object_api_name):
    fields = FIELD_MAP[object_api_name]
    print(fields)
    print(object_api_name)
    return fields

def organize_data(df):
    data = []
    for row in df:
        data.append({'email': row['email'], 'data': {'census_unique_identifier': row['unique_identifier']}})
    return data

def call_bulk_api(object_api_name, keys, df, columns):
    results = []
    data = organize_data(df)
    
    headers = {'Authorization': os.environ.get('SERVICE_AUTHORIZATION_TOKEN'), 'Content-Type': 'application/json'} 
    for row in data:
        requests.request("POST",os.environ.get('WEBHOOK_URL'), headers=headers, data = json.dumps(row))
        results.append({'identifier': row['unique_identifier'], 'success': True})
    return results
    

def list_fields(params):
    result_fields = []

    api_name = params['object']['object_api_name']
    fields = get_fields(api_name)
    print(fields)
    for column in fields:
        census_type = column['type']
        census_array = False

        field = {
            "field_api_name": column['name'],
            "label": column['name'],
            "identifier": False,
            "required": True,  # These need to be based on your implementation of your object
            "createable": True, # These need to be based on your implementation of your object
            "updateable": True, # These need to be based on your implementation of your object
            "type": census_type,
            "array": census_array
        }
        result_fields.append(field)

    return result_fields


def supported_operations(params):
    object = params['object']
    return OPERATION_MAP[object['object_api_name']]

def sync_batch(params, id):
    sync_operation, object_api_name = params['sync_plan']['operation'], params['sync_plan']['object']['object_api_name']
    schema = params['sync_plan']['schema']
    key, columns = None, []
    for col, val in schema.items():
        if val['active_identifier']: 
            key = col
        columns.append({'name': col, 'type': val['field']['type']})

    df = params['records']
    keys = [row[key] for row in df]
    results = call_bulk_api(object_api_name, keys, df, columns)
    return results


@app.route('/census-custom-api', methods = ['POST'])
def run_method_router():
    try:
        # When configuring the Custom API in Census this should be specified as 
        # http://myurl.com/census-custom-api?census-api-key=S3CR3TT0K3N 
        # Thusly, the following line will return "S3CR3TT0K3N"
        auth_variable = request.args.get('census-api-key') 
        jsonrpc, method, id, params, validated = validate_authorization(request, auth_variable)
        if validated:
            if method == 'test_connection':
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': True}})
            elif method == 'list_objects':
                objects = list_objects()
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'objects': objects}})
            elif method == 'list_fields':
                fields = list_fields(params)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'fields': fields}})
            elif method == 'supported_operations':
                operations = supported_operations(params)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'operations': operations}})
            elif method == 'get_sync_speed':
                maximum_batch_size, maximum_parallel_batches, maximum_records_per_second = MAX_BATCH_SIZE, MAX_PARALLEL_BATCHES, MAX_RECORDS_PER_SECOND
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'maximum_batch_size': maximum_batch_size, 'maximum_parallel_batches': maximum_parallel_batches, 'maximum_records_per_second': maximum_records_per_second}})
            elif method == 'sync_batch':
                sync_results = sync_batch(params, id)
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'record_results': sync_results}})
            else:
                return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': False, 'error_message': 'That method is not supported'}})
        else:
            return json.dumps({'jsonrpc': jsonrpc, 'id': id, 'result': {'success': False, 'error_message': 'The API Key is invalid'}})
    except Exception as e:
        print('Error '+ e)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
