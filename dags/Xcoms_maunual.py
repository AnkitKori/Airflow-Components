from airflow.sdk import dag, task

@dag(dag_id="xcom_manual_dag")
def xcom_manual_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = {"data": [1,2,3,4]}
        ti.xcom_push(key = 'return_result', value = fetched_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')['data']
        transformed_data = fetched_data * 2
        transform_dict =  {"transform_data": transformed_data}
        ti.xcom_push(key = 'return_result', value = transform_dict)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        loaded_data = ti.xcom_pull(task_ids = 'second_task', key = 'return_result')
        return loaded_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

xcom_manual_dag()