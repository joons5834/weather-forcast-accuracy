import os
from datetime import timedelta, timezone, datetime
import requests

from google.cloud import storage

session = requests.Session()
client = storage.Client()
URL = os.environ['URL']
KEY = os.environ['KEY']
bucket_name = os.environ['BUCKET'] #without gs://
folder_name = os.environ['FOLDER_NAME']
KST = timezone(timedelta(hours=9), name='KST')

def get_current_kma_hourly(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     # get bucket
     bucket = client.get_bucket(bucket_name)

     # download the file to Cloud Function's tmp directory
     ts_now = datetime.now(tz=KST)

     params = {
          'serviceKey': KEY,
          #numOfRows:10,
          #pageNo:1,
          'base_date': ts_now.strftime("%Y%m%d"),#20210725,
          'base_time': ts_now.strftime("%H00"),#1400,
          'nx':os.environ['nx'],
          'ny':os.environ['ny'],
          'dataType':'JSON'
     }
     response = session.get(URL, params=params)
     response.raise_for_status()

     file_name = folder_name + '/' + ts_now.strftime("kma_obs_%Y-%m-%d_%H-%M_KST.json")

     # set Blob
     blob = storage.Blob(file_name, bucket)

     # upload the file to GCS
     blob.upload_from_string(
          data=response.text,
          content_type='application/json'
     )

     print("""This Function was triggered by messageId {} published at {}
     """.format(context.event_id, context.timestamp))
