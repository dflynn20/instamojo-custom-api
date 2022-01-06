# Census Custom API
This is a way to modularize a Python Flask microservice to be compatible with Census via the Custom API. This serves as a basis to create a [Flask](https://flask.palletsprojects.com/en/2.0.x/) microservice to sync to custom destinations via Census' Custom API [documentation](https://docs.getcensus.com/destinations/custom-api).

This code needs modifications based on your specific use case. Namely, you will need to properly configure the following variables based on your use case.

- `MAX_BATCH_SIZE`: How many records should be synced in a singular batch
- `MAX_PARALLEL_BATCHES`: How many batches can your API run in parallel
- `MAX_RECORDS_PER_SECOND`: Specified by the service API
- `ENDPOINTS`: Which endpoints do you want to impact
- `OPERATION_MAP`: Mapping endpoints to the operations that your API will support

Additionally, this assumes that you will have three environment variables when this is deployed, or locally in a `.env` file.
- `BASE_URL`: Base URL that you will be impacting for service connections
- `CENSUS_AUTHORIZATION_TOKEN`: Called in `middleware.py`, the token that is passed in the URL param
- `SERVICE_AUTHORIZATION_TOKEN`: Specified by the service API for authentication downstream

Hopefully this will serve as a good starting point to get up and running with Census Custom API, please feel free to write to us at support@getcensus.com.
