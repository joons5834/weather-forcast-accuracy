import os
from datetime import timedelta, timezone, datetime
import json
import requests
from time import sleep

from google.cloud import storage

MAX_TRIES = 3
is_response_valid = False
client = storage.Client()
URL = os.environ['URL']
KEY = os.environ['KEY']
bucket_name = os.environ['BUCKET'] #without gs://
folder_name = os.environ['FOLDER_NAME']
KST = timezone(timedelta(hours=9), name='KST')
xys = json.loads(os.environ['NX_NYS'])

def get_ultrashort_forecast_hourly(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     # get bucket
     bucket = client.get_bucket(bucket_name)

     # download the file to Cloud Function's tmp directory
     for nx, ny in xys:
      ts_now = datetime.now(tz=KST)

      params = {
            'serviceKey': KEY,
            'numOfRows':60,
            'pageNo':1,
            'base_date': ts_now.strftime("%Y%m%d"),#20210725,
            'base_time': ts_now.strftime("%H00"),#1400,
            'nx':nx,
            'ny':ny,
            'dataType':'JSON'
      }
      is_response_valid = False
      tries = MAX_TRIES
      while not is_response_valid and tries > 0:
       response = requests.get(URL, params=params)
       if len(response.content) >= 1000:
        is_response_valid = True
        print('valid file size')
       else:
        print('invalid file size. Retrying')
        sleep(1)
       tries -= 1

      response.raise_for_status()
      if not is_response_valid:
       print('writing error message into a file.')
       

      file_name = folder_name + '/' + ts_now.strftime(f"kma_usfc_{nx}_{ny}_%Y-%m-%d_%H-%M_KST.json")

      # set Blob
      blob = storage.Blob(file_name, bucket)

      # upload the file to GCS
      blob.upload_from_string(
            data=response.text,
            content_type='application/json'
      )

     print("""This Function was triggered by messageId {} published at {}
     """.format(context.event_id, context.timestamp))
