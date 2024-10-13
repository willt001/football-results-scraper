from airflow.decorators import task, dag
from pendulum import datetime
from datetime import timedelta
from scraper import scrape_results, date_range
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from constants import BUCKET_NAME
import os

@dag(
    start_date=datetime(2024, 10, 7),
    schedule_interval='@weekly',
    catchup=False
)
def football_results_etl():

    @task
    def extract_results(ti=None):
        end_date = ti.execution_date.date()
        start_date = end_date - timedelta(6)
        fixture_dates = date_range(start_date, end_date)
        for fixture_date in fixture_dates:
            scrape_results(fixture_date=fixture_date, output_root_path=f'{ti.dag_id}_{ti.execution_date.date()}')
        
    @task
    def get_s3_kwargs(ti=None):
        files = os.listdir(f'{ti.dag_id}_{ti.execution_date.date()}')
        kwargs = []
        for file in files:
            kwargs.append(
                {
                "filename": f"{ti.dag_id}_{ti.execution_date.date()}/{file}",
                "dest_key": f"{ti.dag_id}/{file.replace('-', '/')}"
                })
        return kwargs
    
    @task.bash(trigger_rule='all_done')
    def delete_local_directory(ti=None):
        return f'rm -rf /opt/airflow/{ti.dag_id}_{ti.execution_date.date()}'
    
    @task
    def upload_to_s3(kwargs, context={}):
        local_to_s3 = LocalFilesystemToS3Operator(          
            task_id='local_to_s3',
            filename=kwargs['filename'],
            dest_key=kwargs['dest_key'],
            dest_bucket=BUCKET_NAME,
            aws_conn_id='aws_access',
            replace=True
        )
        local_to_s3.execute(context)
                                                    
    extract_results() >> upload_to_s3.expand(kwargs=get_s3_kwargs()) >> delete_local_directory()
    
football_results_etl()