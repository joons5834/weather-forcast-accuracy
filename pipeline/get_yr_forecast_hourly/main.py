import os
from datetime import datetime
import json
import requests
from time import sleep

from google.cloud import storage

MAX_TRIES = 3
is_response_valid = False
client = storage.Client()
ENDPOINT = os.environ['ENDPOINT'] # https://api.met.no/weatherapi/locationforecast/2.0/complete
USER_AGENT = os.environ['USER_AGENT']
bucket_name = os.environ['BUCKET'] #without gs://
folder_name = os.environ['FOLDER_NAME']
POINTS = json.loads(os.environ['POINTS']) # [[POINT_X, POINT_Y],....]

def get_hourly_forecast_metno_gcs(request):
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
    
    for lat, lon in POINTS:
        ts_now = datetime.now()
        headers = {'User-Agent': USER_AGENT}
        last_mod_file_name = folder_name + '/last_modified/' + f'{lat}_{lon}'
        for blob in client.list_blobs(bucket, prefix=last_mod_file_name):
            blob = storage.Blob(last_mod_file_name, bucket)
            headers['If-Modified-Since'] = blob.download_as_bytes().decode('utf-8')
        params = {'lat': lat, 'lon': lon}
        is_response_valid = False
        is_modified = True
        tries = MAX_TRIES

        while not is_response_valid and tries > 0 and is_modified:
            try:
                response = requests.get(ENDPOINT, headers=headers, params=params)
                response.raise_for_status()
                if response.status_code == 304:
                    print("Not Modified")
                    is_modified = False
            except BaseException as e:
                print(e)
                print('API error. Retrying')
                sleep(1)
                tries -= 1
                continue
            is_response_valid = True

        if not is_response_valid:
            raise Exception('API error.')
            
        if not is_modified:
            print(f'{lat} {lon} not modified at {ts_now}')
            continue
            

        file_name = folder_name + '/' + ts_now.strftime(f"metno_hr_fc_{lat}_{lon}_%Y-%m-%d_%H-%M.json")

        # set Blob
        blob = storage.Blob(file_name, bucket)

        # upload the file to GCS
        blob.upload_from_string(
            data=response.text,
            content_type='application/json'
        )

        blob = storage.Blob(last_mod_file_name, bucket)
        blob.upload_from_string(
            data=response.headers['Last-Modified']
        )

    return 'The extract job to gcs is done'