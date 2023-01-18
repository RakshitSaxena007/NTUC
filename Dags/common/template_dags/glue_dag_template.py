
import sys
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/variables")
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/Config")


from config import *
from variable import *

from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


class DyynamicDag:
    def create_dag(self,task_config,elements,dag_num):
        dag=DAG(
                dag_id=f"{task_config[DAG_PARAMETER][DAG_NAME]}_{elements[TABLE_ID]}_{dag_num}",
                description=task_config[DAG_PARAMETER][DAG_DESCRIPTION],
                default_args=task_config[DAG_PARAMETER][DEFAULT_ARGS],
                schedule_interval=task_config[DAG_PARAMETER][SCHEDULE_INTERVAL] 
        )
        with dag:

            start_task = DummyOperator(task_id='start')

            glue_etl = GlueJobOperator(
            task_id=f'{elements[TABLE_ID]}',
            job_name =f'{task_config[GLUEJOBNAME]}',
            script_args={
                "--sourceschema":f"{elements[GLUE_PARAMS][SOURCE_SCHEMA]}",
                "--targetschema":f"{elements[GLUE_PARAMS][TARGET_SCHEMA]}",
                "--sourcepath":f"{elements[GLUE_PARAMS][SOURCE_PATH]}",
                "--targetpath":f"{elements[GLUE_PARAMS][SOURCE_PATH]}"
            },
            iam_role_name=f'{task_config[ROLENAME]}',
        
            )
        
            end_task = DummyOperator(task_id='end_task')
    
            start_task >> glue_etl >> end_task
        return dag
    
       