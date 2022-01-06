import os

# Uncomment if running locally

# from dotenv import load_dotenv, find_dotenv
# load_dotenv(find_dotenv())


def validate_authorization(request, auth_variable):
    validated = os.environ.get("CENSUS_AUTHORIZATION_TOKEN") == auth_variable
    data = request.json
    jsonrpc, method, id, params = data['jsonrpc'], data['method'], data['id'], data['params']
    return jsonrpc, method, id, params, validated
