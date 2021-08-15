import os
from datetime import datetime
import requests
from google.cloud import storage

URL = os.environ['OPENWEATHER_URL']
bucket_name = os.environ['BUCKET'] #without gs://
API_KEY = os.environ['OPENWEATHER_KEY']


def import_file(event, context):

	# set storage client
	client = storage.Client()

	# get bucket
	bucket = client.get_bucket(bucket_name)

	# download the file to Cloud Function's tmp directory
	params = {'lat':37.642117,'lon':126.935432,'appid': API_KEY, 'exclude': 'minutely,alerts'}
	r = requests.get(URL, params=params)
	r.raise_for_status()
	t = datetime.utcnow()
	file_name = t.strftime("%m-%d-%Y_%H-%M-%S") + '.json'
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