from airflow.sdk import dag, task

@dag(dag_id = "parallel_dag")
def parallel_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data": [1,2,3],
                                "db_extracted_data": [4,5,6],
                                "s3_extarcted_data": [7,8,9]}
        ti.xcom_push(key = "return_value", value = extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids = 'extract_task')['api_extracted_data']
        print(f"Transforming API data: {api_extracted_data}")
        transormed_api_data = [i * 10 for i in api_extracted_data]
        ti.xcom_push(key = "return_value", value = transormed_api_data)


    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids = 'extract_task')['db_extracted_data']
        print(f"Transforming DB data: {db_extracted_data}")
        transormed_db_data = [i * 100 for i in db_extracted_data]
        ti.xcom_push(key = "return_value", value = transormed_db_data)


    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids = 'extract_task')['s3_extarcted_data']
        print(f"Transforming S3 data: {s3_extracted_data}")
        transormed_s3_data = [i * 1000 for i in s3_extracted_data]
        ti.xcom_push(key = "return_value", value = transormed_s3_data)


    @task.bash
    def load_data(**kwargs):
        print("Loaded data to destination")
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids='transform_task_api')
        db_data = ti.xcom_pull(task_ids='transform_task_db')
        s3_data = ti.xcom_pull(task_ids='transform_task_s3')

        return f"echo 'Loaded data: {api_data}, {db_data}, {s3_data}'"
        

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_data()

    extract >> [transform_api, transform_db, transform_s3] >> load

parallel_dag()