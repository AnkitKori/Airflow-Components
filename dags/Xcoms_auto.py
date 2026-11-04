from airflow.sdk import dag, task

@dag(dag_id = "xcom_auto_dag")
def xcom_auto_dag():

    @task.python
    def first_task():
        fetched_data = {"data": [1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data: dict):
        transformed_data = data['data']
        transformed_data = transformed_data * 2
        return {"transform_data": transformed_data}

    @task.python
    def third_task(data: dict):
        loaded_data = data
        return loaded_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcom_auto_dag()