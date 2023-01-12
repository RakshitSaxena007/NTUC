

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
    
        # Create a default_args dict for the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('1',start_date=datetime(2022,12,20),
              schedule_interval='@daily')

start_task = DummyOperator(task_id='start_task', dag=dag)

        
glue_etl = GlueJobOperator(
        task_id="glue_etl_1",
        job_name ='test_airflow_job_1',
        script_args={"--schemaa":'test',
        "--target":'s3/path',
        "--other":'otherconfig',
        "--sourcedest":'s3/anotherpath',
        },
        iam_role_name='roleee',
    
        dag=dag)
    
end_task = DummyOperator(task_id='end_task', dag=dag)
 
start_task >> glue_etl >> end_task
    
       