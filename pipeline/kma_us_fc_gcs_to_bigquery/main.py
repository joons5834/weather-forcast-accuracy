import os
import json
# from time import strptime

from google.cloud import bigquery
from google.cloud import storage

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
DATASET_ID = 'raw_data' # os.environ['DATASET']
TABLE_ID = 'ultrashort_fc_kma' # os.environ['TABLE']
PROJECT_ID = 'weather-forecast-accuracy'
storage_client = storage.Client()
bq_client = bigquery.Client()

def load_csv_to_bq(data, context):
        dataset_ref = bq_client.dataset(DATASET_ID) # dataset_id here
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_APPEND'
        job_config.schema = bq_client.schema_from_json(r'dt_raw_ultrashort_fc_kma.json')

        # get blobs from the directory excluding subdirectories and their files
        bucket_name = 'weather-forecasts-for-eval'
        blobs = list(storage_client.list_blobs(bucket_name, prefix='kma/test_us/', delimiter='/'))
        # print('blobs:', [blob.name for blob in blobs])
        # weather-forecasts-for-eval/kma/test_us
        # parse JSON file 
        json_rows = []
        invalid_files = []
        for blob in blobs:
            try:
                d = json.loads(blob.download_as_string(client=None))
                rows = d['response']['body']['items']['item']
                for row in rows:
                    # print('row to be added:', row)
                    bdate = row['baseDate']
                    btime = row['baseTime']
                    fdate = row['fcstDate']
                    ftime = row['fcstTime']
                    row['baseTimestamp'] = f'{bdate[:4]}-{bdate[4:6]}-{bdate[6:]} {btime[:2]}:{btime[2:]}+09:00'
                    row['fcstTimestamp'] = f'{fdate[:4]}-{fdate[4:6]}-{fdate[6:]} {ftime[:2]}:{ftime[2:]}+09:00'
                    del row['baseDate']
                    del row['baseTime']
                    del row['fcstDate']
                    del row['fcstTime']
                    json_rows.append(row)
                    # print('row added:', row)
            except BaseException as e:
                # print('Invalid json:', blob.name)
                invalid_files.append((blob.name, repr(e)))
                raise e
        
        print('Finished json parsing.')
        if invalid_files:
            print('Check errors for', invalid_files)
        # load the data into BQ
        load_job = bq_client.load_table_from_json(
                json_rows=json_rows,#: Iterable[Dict[str, Any]]
                destination=dataset_ref.table(TABLE_ID),
                project=PROJECT_ID,
                job_config=job_config)

        load_job.result()  # wait for table load to complete.
        print('Load job finished.')

        # move files to '/done'
        bucket = storage_client.bucket(bucket_name)
        rename_errors = []
        for blob in blobs:
            try:
                last_slash_idx = blob.name.rfind('/')
                new_name = blob.name[:last_slash_idx] + '/done' + blob.name[last_slash_idx:]
                bucket.rename_blob(blob, new_name)
            except:
                # print('Error occurred renaming', blob.name, '->', new_name)
                rename_errors.append(blob.name)
                continue
        
        print("Blob migration finished.")
        if rename_errors:
            print('Check errors for', rename_errors)
  
        print('Job finished.')