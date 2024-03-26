import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_simple_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 26, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''서울시 공공자전거 실시간 이용정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id ='tb_cycle_station_info',
        http_conn_id = 'openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul.go.kr}}/json/bikeList/1/5/',
        method='GET',
        header={'Content-Type' : 'application/json',
                'charset':'utf-8',
                'Accept':'*/*'
                }
    )
    
    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rst = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint
        pprint(json.loads(rst))

    tb_cycle_station_info >> python_2()