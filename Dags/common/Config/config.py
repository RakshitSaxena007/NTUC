import sys
sys.path.append("/Users/rakshitsaxena/airflow/Dags/AirflowDMS/Dags/common/variables")
from variable import *
from datetime import timedelta
from datetime import datetime

COMMON_CONFIG = {
    GLUEJOBNAME:"nameofjob",
    ROLENAME:"nameofrole",
    REGION: "region_name",
    DAG_PARAMETER: {
        DAG_NAME: "dms_table",
        DAG_DESCRIPTION: "Dag for Raw to Curated",
        DEFAULT_ARGS: {
            OWNER: "CloudCover",
            START_DATE: datetime(2022,12,12),
            RETRIES: 1,
            RETRY_DELAY: timedelta(minutes=3),
            CATCH_UP: False,
            PROVIDE_CONTEXT: True,
            # TEMPLATE_SEARCHPATH: '/path'
        },
        SCHEDULE_INTERVAL: "30 3 * * *"
    }
}


TABLE_CONFIG = [
        {DMS_FIRST_TABLE_NAME:'',
        TABLE_ID:TABLE_ID,
        GLUE_PARAMS:{
            SOURCE_SCHEMA:"",
            TARGET_SCHEMA:"",
            SOURCE_PATH:"",
            TARGET_PATH:""
            }
        },
        {DMS_FIRST_TABLE_NAME:'',
        TABLE_ID:TABLE_ID,
        GLUE_PARAMS:{
            SOURCE_SCHEMA:"",
            TARGET_SCHEMA:"",
            SOURCE_PATH:"",
            TARGET_PATH:""
            }
        },
       {DMS_FIRST_TABLE_NAME:'',
        TABLE_ID:TABLE_ID,
        GLUE_PARAMS:{
            SOURCE_SCHEMA:"",
            TARGET_SCHEMA:"",
            SOURCE_PATH:"",
            TARGET_PATH:""
            }
        }
]
