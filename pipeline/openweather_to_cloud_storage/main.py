import os
from datetime import datetime
import json
import requests
from google.cloud import storage

URL = os.environ['OPENWEATHER_URL']
bucket_name = os.environ['BUCKET'] #without gs://
API_KEY = os.environ['OPENWEATHER_KEY']
LOCATIONS = json.loads(os.environ['LOCATIONS']) # [[lat1,lon1], [lat2,lon2],...]

client = storage.Client() # set storage client
bucket = client.get_bucket(bucket_name) # get bucket

def import_file(event, context):
    for lat, lon in LOCATIONS:
        # download the file to Cloud Function's tmp directory
        params = {'lat':lat,'lon':lon,'appid': API_KEY, 'exclude': 'minutely,alerts'}
        r = requests.get(URL, params=params)
        r.raise_for_status()
        t = datetime.utcnow()
        file_name = t.strftime(f"%m-%d-%Y_%H-%M-%S_{lat}_{lon}") + '.json'
        # cf_path = '/tmp/{}'.format(file_name)
        # with open(file_name, mode='w') as f:
        # 	f.write(r.text + '\n')
        
        # set Blob
        blob = storage.Blob(file_name, bucket)
    
        # upload the file to GCS
        # blob.upload_from_filename(cf_path)
        blob.upload_from_string(
            data=r.text,
            content_type='application/json'
        )

    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))