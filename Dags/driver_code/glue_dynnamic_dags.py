
import sys
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/variables")
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/Config")
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/template_dags")

from glue_dag_template import DyynamicDag
from variable import *
from config import COMMON_CONFIG,TABLE_CONFIG

"""
Create dynamic DAG for data validation
"""


'DAG'

user_config = COMMON_CONFIG
dag_class = DyynamicDag()
# print(dag_class)
count = 0
for elements in TABLE_CONFIG:
    count = count+1
    globals()[f"{user_config[DAG_PARAMETER][DAG_NAME]}_{elements[TABLE_ID]}_{count}"] \
        = dag_class.create_dag(task_config=user_config,elements=elements,dag_num=count)

