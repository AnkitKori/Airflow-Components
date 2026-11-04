from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id = "incremental_load_dag",
    schedule=CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
    start_date=datetime(year=2026, month=10, day=27, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=10, day=31, tz="Asia/Kolkata"),
    catchup= True
)

def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        data_interval_start = kwargs['data_interval_start']
        data_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {data_interval_start} to {data_interval_end}")

    
    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"

    fetch_data = incremental_data_fetch()
    process_data = incremental_data_process()

    fetch_data >> process_data

incremental_load_dag()
    