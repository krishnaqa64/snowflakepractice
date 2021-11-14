import pandas as pd
import numpy as np
import os
import snowflake.connector as sf
from snowflake.connector import DictCursor
from config import username,password ,account ,schema


def execute_query1(query,database,warehouse):
    conn1 = sf.connect(user=username, password=password, account=account)
    sql1 = 'use {}'.format(database)
    sql2 = 'use WAREHOUSE {}'.format(warehouse)
    cursor = conn1.cursor(DictCursor)
    cursor.execute(sql1)
    cursor.execute(sql2)
    cursor.execute(query)
    return cursor

def execute_query(query,database,warehouse):
    conn1 = sf.connect(user=username, password=password, account=account)
    sql1 = 'use {}'.format(database)
    sql2 = 'use WAREHOUSE {}'.format(warehouse)
    cursor = conn1.cursor()
    cursor.execute(sql1)
    cursor.execute(sql2)
    cursor.execute(query)
    results= cursor.fetch_pandas_all()
    col_names=[column[0] for column in cursor.description]
    result=list(map(lambda row:dict(zip(col_names, row)),results))
    return result



# database="TEST_DB"
# warehouse1 = "TEST_WAREHOUSE"
# warehouse2='COMPUTE_SH'
#
# sql3 = 'select * from PUBLIC.TEMP_TABLE'
# print(execute_query(sql3,database,warehouse1))
#
