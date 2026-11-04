from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    # This is optional but good to include for clarity about assest's location
    uri="/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)

def fetch_data(self):

    # Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # Simulate data fetching by writing to the file
    with open(self.uri, 'w') as f:
        f.write(f"Data fetched Successfully")

    print(f"Data written to {self.uri}")
