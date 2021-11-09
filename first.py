import os
import yaml

import snowflake.connector as sf
from snowflake.connector import DictCursor
import pandas as pd
import numpy as np
import json

username = "KRISHNAQA64"
password= ""
account= 'au99370.canada-central.azure'
warehouse1 = "TEST_WAREHOUSE"
warehouse2='COMPUTE_SH'
database="TEST_DB"
schema='PUBLIC'

conn=sf.connect(user=username,password=password,account=account,database=database,schema=schema)
curs=conn.cursor()
curs.execute("USE ROLE SYSADMIN")
SQL='select * from "PUBLIC"."TEMP_TABLE"'
curs.execute(SQL)
df=curs.fetch_pandas_all()
print(df)
#
#
# print("\n")
#
# print("Cursor :- \n")
#
# conn1=sf.connect(user=username,password=password,account=account)
# def execute_query(connection,query):
#     cursor=connection.cursor()
#     cursor.execute(query)
#     cursor.close()
#
# try:
#     sql='use {}'.format(database)
#     execute_query(conn1,sql)
#     sql = 'use WAREHOUSE {}'.format(warehouse1)
#     execute_query(conn1, sql)
#
#     # sql = 'alter WAREHOUSE {} resume'.format(warehouse1)
#     # execute_query(conn1, sql)
#     sql = "insert into PUBLIC.TEMP_TABLE values ('4','john','Newyork')"
#     cursor = conn1.cursor(DictCursor)
#     cursor.execute(sql)
#     for c in cursor:
#         print(c)
#     cursor.close
#
#     print("\n")
#
#     sql = 'select * from PUBLIC.TEMP_TABLE'
#     cursor=conn1.cursor(DictCursor)
#     cursor.execute(sql)
#     for c in cursor:
#         print(c)
#     cursor.close
#
# except Exception as e:
#     print(e)
#
# @testdb_mg.testschema_mg.%test_table")
# conn.cursor().execute("PUT file://./data/crick* @testdb_mg.testschema_mg.%test_table")
# conn.cursor().execute("""COPY INTO test_table from @testdb_mg.testschema_mg.%test_table/crick*.csv.gz  file_format = (type = csv field_delimiter=',') pattern = '.*.csv.gz' on_error= 'skip_file'""")
# # For S3

print(os.listdir())

