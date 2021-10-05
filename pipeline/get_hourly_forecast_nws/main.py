import os
from datetime import datetime
import json
import requests
from time import sleep

from google.cloud import storage

MAX_TRIES = 3
is_response_valid = False
client = storage.Client()
ENDPOINT_PREFIX = os.environ['ENDPOINT_PREFIX'] # https://api.weather.gov/gridpoints/
ENDPOINT_SUFFIX = os.environ['ENDPOINT_SUFFIX'] # /forecast/hourly
USER_AGENT = os.environ['USER_AGENT']
bucket_name = os.environ['BUCKET'] #without gs://
folder_name = os.environ['FOLDER_NAME']
GRIDS = json.loads(os.environ['GRIDS']) # [[GRID_ID,GRID_X,GRID_Y],....]

def get_hourly_forecast_nws(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    # get bucket
    bucket = client.get_bucket(bucket_name)

    # download the file to Cloud Function's tmp directory
    for grid_id, grid_x, grid_y in GRIDS:
        ts_now = datetime.now()
        headers = {
            'User-Agent': USER_AGENT,
            
            'Accept': 'application/geo+json',
        }
        params = {'units': 'si'}
        is_response_valid = False
        tries = MAX_TRIES

        while not is_response_valid and tries > 0:
            try:
                response = requests.get(ENDPOINT_PREFIX + f'{grid_id}/{grid_x},{grid_y}{ENDPOINT_SUFFIX}',
                headers=headers, params=params)
                response.raise_for_status()
                if len(response.content) < 1000:
                    raise Exception('invalid file size.')
            except BaseException as e:
                print(e)
                print('API error. Retrying')
                sleep(1)
                tries -= 1
                continue
            is_response_valid = True

        if not is_response_valid:
            print('writing error message into a file.')
            

        file_name = folder_name + '/' + ts_now.strftime(f"nws_hr_fc_{grid_x}_{grid_y}_%Y-%m-%d_%H-%M.json")

        # set Blob
        blob = storage.Blob(file_name, bucket)

        # upload the file to GCS
        blob.upload_from_string(
            data=response.text,
            content_type='application/geo+json'
        )

    return 'The extract job to gcs is done'