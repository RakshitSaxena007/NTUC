import openpyxl
from pathlib import Path
import pandas as pd
import boto3
import time
# mapping excel file with sheet names
def file_sheet_mapping(directory):
    file_name_sheet={}
    files = Path(directory).glob('*')
    for file in files:
        w=openpyxl.load_workbook(file)
        t=str(file)
        r=t.split("/")
        filenmae=r[-1]
        file_name_sheet[filenmae]=w.sheetnames
    #remove unessasry sheets
    
    for i,j in file_name_sheet.items():
        if j[0].startswith("DMS")or j[0].startswith("Dms") or j[0].startswith("dms"):
            j.pop(0)
    return file_name_sheet
directory = '/Users/rakshitsaxena/Documents/DMS/'
file_name_sheet=file_sheet_mapping(directory)

# mapping target schema with the table name
def target_schema_mapping(directory):
    df=pd.read_excel(directory)
    w=df[["Table name","Target Schema"]]
    table_name_target={}
    final_table_target={}
    for index, row in w.iterrows():
        table_name_target[row['Table name']]=row["Target Schema"]
    for i,j in table_name_target.items():
        final_table_target[i.upper()]=j
    return final_table_target
directory="/Users/rakshitsaxena/Documents/DMS/DMS_Oracle_EBGI_CUT1.xlsx"
final_table_target=target_schema_mapping(directory)
# data type mapping dictonary compatible with hive and iceberg can add more if needed
datatypemapping={"VARCHAR2":"STRING",
"CHAR":"STRING",
"NUMBER":"INT",
"VARCHAR":"STRING",
"SMALLDATETIME":"timestamp",
"TIME":"timestamp",
"DATE":"date",
"DATETIME":"timestamp"}
# source statements creation
def source_ddl_creation(source_bucket_name,folderdirectory,datatypemapping):
    source_statments={}
    statement_prefix=""
    for i,j in file_name_sheet.items():
        d=pd.read_excel(folderdirectory+str(i),sheet_name=j)
        for k in d:
            r=pd.DataFrame(d[str(k)][["COLUMN_NAME","DATA_TYPE","DATA_LENGTH","NULLABLE"]])
            tablename=d[str(k)]["TABLE_NAME"][0]
            schemaname=d[str(k)]["SCHEMA_NAME"][0]
            statement_prefix = "CREATE EXTERNAL TABLE IF NOT EXISTS " +schemaname+'.'+tablename + " ("
            for ind,row in r.iterrows():
                if ind==len(r)-1:
                    if str(row["DATA_TYPE"]).upper() in datatypemapping:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ datatypemapping[dt]
                    else:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ dt
                else:
                    if str(row["DATA_TYPE"]).upper() in datatypemapping:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ datatypemapping[dt]+","
                    else:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ dt+","
            statement_prefix+=")"
            statement_prefix+="\n" + "LOCATION 's3://"+source_bucket_name+"/"+str(schemaname)+"/"+str(tablename)+"';"
            source_statments[tablename]=statement_prefix
    return source_statments
folderdirectory="/Users/rakshitsaxena/Documents/DMS/"
source_bucket_name="ntuc-poc"
source_statments=source_ddl_creation(source_bucket_name,folderdirectory,datatypemapping)

# target statment creation

def target_ddl_creation(target_bucket_name,folderdirectory,datatypemapping,final_table_target):
    target_statements={}
    statement_prefix=""
    for i,j in file_name_sheet.items():
        d=pd.read_excel(folderdirectory+str(i),sheet_name=j)
        for k in d:
            r=pd.DataFrame(d[str(k)][["COLUMN_NAME","DATA_TYPE","DATA_LENGTH","NULLABLE"]])
            tablename=d[str(k)]["TABLE_NAME"][0]
            schemaname=final_table_target[tablename.upper()]
            statement_prefix = "CREATE TABLE IF NOT EXISTS " +schemaname+'.'+tablename + " ("
            for ind,row in r.iterrows():
                if ind==len(r)-1:
                    if str(row["DATA_TYPE"]).upper() in datatypemapping:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ datatypemapping[dt]
                    else:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ dt
                else:
                    if str(row["DATA_TYPE"]).upper() in datatypemapping:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ datatypemapping[dt]+","
                    else:
                        dt=str(row["DATA_TYPE"]).upper()
                        statement_prefix+="\n"+str(row["COLUMN_NAME"])+" "+ dt+","
            statement_prefix+=")"
            statement_prefix+="\n" + "LOCATION 's3://"+target_bucket_name+"/"+str(schemaname)+"/"+str(tablename)+"' \n "
            statement_prefix+="TBLPROPERTIES ( \n "+"'table_type'='ICEBERG', \n"+"'format'='parquet', \n"+"'write_target_data_file_size_bytes'='536870912', \n "+"'optimize_rewrite_delete_file_threshold'='10' \n );"
            target_statements[tablename]=statement_prefix
    return target_statements
folderdirectory="/Users/rakshitsaxena/Documents/DMS/"
target_bucket_name="ntuc-poc"
target_statements=target_ddl_creation(target_bucket_name,folderdirectory,datatypemapping,final_table_target)

# database creation statments
def database_creation_source(directory):
    lst=[]
    for i,j in file_name_sheet.items():
        d=pd.read_excel(directory+str(i),sheet_name=j)
        for k in d:
            schemaname=d[str(k)]["SCHEMA_NAME"][0]
            if schemaname not in lst:
                lst.append(schemaname)
    final_source=[]
    for i in lst:
        s=""
        s+="CREATE DATABASE IF NOT EXISTS "+i+";"
        final_source.append(s)
    return final_source
directory="/Users/rakshitsaxena/Documents/DMS/"
final_source=database_creation_source(directory)

def database_creation_target(directory,final_table_target):
    lst=[]
    for i,j in file_name_sheet.items():
        d=pd.read_excel(directory+str(i),
                      sheet_name=j)
        for k in d:
            tablename=d[str(k)]["TABLE_NAME"][0]
            schemaname=final_table_target[tablename.upper()]
            if schemaname not in lst:
                lst.append(schemaname)
    final_target=[]
    for i in lst:
        s=""
        s+="CREATE DATABASE IF NOT EXISTS "+i+";"
        final_target.append(s)
    return final_target
directory="/Users/rakshitsaxena/Documents/DMS/"
final_target=database_creation_target(directory,final_table_target)

# writing all statements in a ddl file
# before executing create a empty 
# target.ddl,
# source.ddl,
# source_database.ddl,
# target_database.ddl in your desired directory
def writing_ddl_statments(target_statements,source_statments,final_source,final_target):
    f = open('target.ddl',"w")
    for i,j in target_statements.items():
        f.write(j+" \n \n ")
    f = open('source.ddl',"w")
    for i,j in source_statments.items():
        f.write(j+" \n \n ")
    f = open('source_database.ddl',"w")
    for i in final_source:
        f.write(i+" \n ")
    f = open('target_database.ddl',"w")
    for i in final_target:
        f.write(i+" \n ")
writing_ddl_statments(target_statements,source_statments,final_source,final_target)

print("DDL created")

##for testing used boto3 but can also use glue jobs
#Running All the statment one by one in athena
#creating a source database then target the source stament and then target statement
def finalrun(target_statements,source_statments,final_source,final_target):
    
    client = boto3.client(
            'athena',
            aws_access_key_id='ASIA4FG7EVU5SWM6QCEB',
            aws_secret_access_key='LB+P8nBlB4DQarsfvOWdaXz3ICZvdamAh5I+il1l',
            region_name ='ap-northeast-1',
            aws_session_token="IQoJb3JpZ2luX2VjEIz//////////wEaCXVzLWVhc3QtMSJHMEUCIQDnsgGYfa7+aHCZd7wfxHftDmih5AFAl8azkdEAhV6GdgIgIUC16z60+JJx2WiUezG4JT5M+QGZY87jidpC+Z/JEy0qmgMI1P//////////ARACGgw4MzU4MzkxMTA0NTkiDHRLK9nIQNyA5LC1QCruAtga/pQYmprlax9fMh70qkRHJpuMOY5sXjxGbCChCf3zCu/dIi3qHZZlRgGVi8DA/iqtIpLcX1CkrkOzomT+kXfUWZsPVSqng4ZWV3Q9lSkXkYMqDTDnH0HmlvuM4UXAlK/y/Iu+Qt6FIrX53HXh3qCaeZ1w2ig+3A1kZcVFTckdfKtDT+OseKFBBhLyqCF72mDk70UsXtI1HOXysLqhI0WetXTcZ8aykwxbIX0s92GYzh8TU9zludBQxVC37JQhBtQ4oa9JIVbheml0ClcLSK11r36LqHqF3Hm1YyuwHDivEHuS/s0yGwL7gXMHDp36DGMngqRxnbCq8by3ohIe7hw8C4dGeApQNmnZr8ErPvVZ8xuQ3ivFrrRVcgBntNuLoLzmI/Ps17foJRe+VEx2uBKjY9weGqJqRm0krue36T1pfrnTB0W4gg20sTurW2K8ZKo9zJTSuE3CrUGeMFghVp9AObENeddNwK6YbFSO1DComtCdBjqmAQzFCqLRHi3Gf26zRwv7Zm6MhbAYth0ghM9pQQFZ1e2wiLj7L71R86h8j0ohAm5YRMJxYBlYWSi+/cePC8HP614BD2xQ/8RxPl/aZv2My5OSSUANL9UZ8S+mjK9kWa/JM5h00lnpmZLXH6DoEEyJtS724USOrZ6xlKnTrdzpDu3ncBJCGSSNCuuV5N4WqUBGqq4Fz0J845ZLN2k0I13feDGnoNBMTRg="
    )

    for i in range(4):
        if i==0:
            for j in final_source:
                response = client.start_query_execution(
                QueryString=j,
                ResultConfiguration={"OutputLocation": "s3://ntuc-poc/querry/"}
                )
                time.sleep(2)
                print(response["QueryExecutionId"])
        if i==1:
            for j in final_target:
                response = client.start_query_execution(
                QueryString=j,
                ResultConfiguration={"OutputLocation": "s3://ntuc-poc/querry/"}
                )
                time.sleep(2)
                print(response["QueryExecutionId"])
        if i==2:
            for ind,j in target_statements.items():
                response = client.start_query_execution(
                QueryString=j,
                ResultConfiguration={"OutputLocation": "s3://ntuc-poc/querry/"}
                )
                time.sleep(4)
                print(response["QueryExecutionId"])

        if i==3:
            for ind,j in source_statments.items():
                response = client.start_query_execution(
                QueryString=j,
                ResultConfiguration={"OutputLocation": "s3://ntuc-poc/querry/"}
                )
                time.sleep(4)
                print(response["QueryExecutionId"])
    return "Tables and Database created in athena"
    
finalrun(target_statements,source_statments,final_source,final_target)