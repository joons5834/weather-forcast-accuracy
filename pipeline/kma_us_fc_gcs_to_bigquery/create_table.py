from google.cloud import bigquery
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
# Construct a BigQuery client object.
client = bigquery.Client()


table_id = "weather-forecast-accuracy.raw_data.no_dt_ultrashort_fc_kma"

schema = client.schema_from_json(r'raw_ultrashort_fc_kma.json') # assumes json file is in this file's directory

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)