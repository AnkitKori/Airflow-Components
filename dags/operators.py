from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="opeartors_dag")
def opeartors_dag():

    @task.python
    def first_task():
        print("This is the first task")


    @task.python
    def second_task():
        print("This is the second task")


    @task.python
    def third_task():
        print("This is the third task")

    @task.bash()
    def bash_task_modern():
        return "echo https://airflow.apache.org/"

    bash_task_old = BashOperator(
        task_id = "bash_task_old",
        bash_command = "echo https://airflow.apache.org/"
    )


    # Defining the dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    bash_modern = bash_task_modern()
    bash_task_old = bash_task_old

    first >> second >> third >> bash_modern >> bash_task_old

# Instantiating the DAG
opeartors_dag()