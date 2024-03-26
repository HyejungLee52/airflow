from operators.seoul_api_to_csv_operator import SeouldApiToCsvOperator
from airflow.models.dag import DAG
import pendulum

with DAG(
    dag_id="dags_seould_api_corona",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="Asia/Seoul"),
    catchup=False
) as dag:
    '''서울시 코로나 19 확진자 발생동향'''
    tb_corona19_count_status = SeouldApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Aisa/Seoul") | ds_nodash}}',
        file_name='TbCorona19CountStatus.csv'
    )

    '''서울시 코로나 19 백신 예방접종 현황'''
    tb_corona19_vaccine_stat_new = SeouldApiToCsvOperator(
        task_id='tb_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Aisa/Seoul") | ds_nodash}}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tb_corona19_vaccine_stat_new