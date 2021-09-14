from google.cloud import bigquery
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
# Construct a BigQuery client object.
client = bigquery.Client()


# Set table_id to the ID of the table to create.
table_id = "weather-forecast-accuracy.raw_data.ultrashort_fc_kma"

schema = client.schema_from_json(r'dt_raw_ultrashort_fc_kma.json') # assumes json file is in this file's directory

table = bigquery.Table(table_id, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.YEAR,
    field="fcstTimestamp"  # name of column to use for partitioning
)

table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)