import os
import yaml

import snowflake.connector as sf
from snowflake.connector import DictCursor
import pandas as pd
import numpy as np
import json

username = "KRISHNAQA64"
password= "try"
account= 'au99370.canada-central.azure'
warehouse1 = "TEST_WAREHOUSE"
warehouse2='COMPUTE_SH'
database="TEST_DB"
schema='PUBLIC'

# conn=sf.connect(user=username,password=password,account=account,database=database,schema=schema)
# curs=conn.cursor()
# curs.execute("USE ROLE SYSADMIN")
# SQL='select * from "PUBLIC"."TEMP_TABLE"'
# curs.execute(SQL)
# df=curs.fetch_pandas_all()
# print(df)
#
#
# print("\n")
#
# print("Cursor :- \n")
#


conn1=sf.connect(user=username,password=password,account=account)
def execute_query(connection,query):
    cursor=connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql='use {}'.format(database)
    execute_query(conn1,sql)
    sql = 'use WAREHOUSE {}'.format(warehouse1)
    execute_query(conn1, sql)

    # sql = 'alter WAREHOUSE {} resume'.format(warehouse1)
    # execute_query(conn1, sql)

    # sql = "insert into PUBLIC.TEMP_TABLE values ('5','Donald','New')"
    # cursor = conn1.cursor(DictCursor)
    # cursor.execute(sql)
    # for c in cursor:
    #     print(c)
    # print("hello")
    # cursor.close

    print("\n")

    sql = 'select * from PUBLIC.TEMP_TABLE'
    cursor=conn1.cursor(DictCursor)
    cursor.execute(sql)
    for c in cursor:
        print(c)
    cursor.close


except Exception as e:
    print(e)




conn=sf.connect(user=username,password=password,account=account,database=database,schema=schema)
curs=conn.cursor()
curs.execute("USE ROLE SYSADMIN")
# curs.execute("PUT 'file:C:\\Users\\ketin\\PycharmProjects\\snowflake\\data.csv' @TEST_DB.PUBLIC.%test_table")
# curs.cursor().execute("""COPY INTO test_table from @TEST_DB.PUBLIC.%test_table/data.csv.gz  file_format = (type = csv, field_delimiter=',') pattern = '.*.csv.gz' on_error= 'skip_file'""")
SQL='select * from "PUBLIC"."TEMP_TABLE"'
curs.execute(SQL)
df=curs.fetch_pandas_all()
print(df)


curs.execute("USE ROLE SYSADMIN")
curs.execute("create or replace file format mycsvformat type = 'CSV' field_delimiter = '|' skip_header = 1")
curs.execute("CREATE OR REPLACE STAGE PUBLIC.MY_STAGE FILE_FORMAT = mycsvformat")

SQL='select current_database(),current_schema(),current_warehouse();'
curs.execute(SQL)
df=curs.fetch_pandas_all()
print("\n")
print(df)


file_path = "file://{0}\data.csv".format(os.getcwd())
print("PUT {0} @TEST_DB.PUBLIC.%MY_STAGE".format(file_path))

#curs.execute("PUT '{0}' @TEST_DB.PUBLIC.MY_STAGE".format(file_path))
# curs.execute("PUT file:///C:\\Users\\ketin\\PycharmProjects\\snowflakedata.csv @TEST_DB.PUBLIC.MY_STAGE".format(file_path))


curs.execute("PUT file://C:\\Users\\ketin\\PycharmProjects\\snowflake\\data.csv @TEST_DB.PUBLIC.MY_STAGE")

SQL='list @TEST_DB.PUBLIC.my_stage'
curs.execute(SQL)
df = pd.DataFrame.from_records(iter(curs), columns=[x[0] for x in curs.description])
print(df)

curs.execute("copy into TEST_DB.PUBLIC.TEMP_TABLE from @my_stage file_format = (type='csv') ,pattern='.*data.csv.gz' on_error='skip_file' ")


SQL='select * from "PUBLIC"."TEMP_TABLE"'
curs.execute(SQL)
df=curs.fetch_pandas_all()

#print(os.listdir())
print(os.getcwd())

print("hello")