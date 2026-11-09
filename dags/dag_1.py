from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def greetings():
    print("Hello, Airflow! Joniel - Engenheiro de Dados")

defalt_args = {
    "owner": "JMelo",  
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#declarar DAG
with DAG(
    dag_id="dag_1",
    description="DAG de exemplo para demonstrar o uso do Airflow",
    tags=["first_dag", "airflow"],
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 8),
    catchup=False,
    default_args=defalt_args,
) as dag:
    start_execution = EmptyOperator(
        task_id="start_execution"
        )

    bash_command = BashOperator(
        task_id="bash_command",
        bash_command="echo 'Hello, Airflow!'"
    )
    greeting_task = PythonOperator(
        task_id="greetings",
        python_callable=greetings
        #trigger_rule="all_done"
    )

    finish_execution = EmptyOperator(
        task_id="finish_execution"
        #trigger_rule="all_done"
        )

#executar as tarefas em sequência
#start_execution >> bash_command >> greeting_task >> finish_execution

#executar as tarefas em paralelo:
start_execution >> [bash_command >> greeting_task] >> finish_execution