
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = run_this = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = run_this = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2